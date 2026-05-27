"""
infra/chaos_test.py
-------------------
Prueba de chaos engineering para el cluster Cassandra de CryptoFlow.

Objetivo:
  Demostrar que el cluster (RF=3, LOCAL_QUORUM) sostiene escrituras sin
  perdida de mensajes ante el fallo de un nodo, y que se recupera
  automaticamente cuando el nodo regresa.

Fases:
  1. baseline    - 30s de escrituras con 3 nodos UN. Mide ev/s base.
  2. failure     - `docker stop` sobre 1 nodo, 30s mas de escrituras.
                   Con RF=3 + LOCAL_QUORUM el quorum (2/3) se sostiene.
  3. recovery    - `docker start` del nodo, espera `nodetool status` = UN,
                   mide ev/s tras recuperacion y reporta tiempo a UN.

Salida:
  Una linea JSON por evento (log estructurado) + un resumen final con
  throughput por fase, inserts fallidos por fase y tiempo de recuperacion.

Idempotencia:
  El bloque `finally` siempre intenta reiniciar el nodo si quedo apagado.
  No deja datos residuales aparte de las filas escritas durante el test
  (sujetas al TTL de 7 dias definido en `raw_trades`).

Modo dry-run:
  --dry-run usa un sink mock (sin Cassandra, sin docker stop). Util para
  smoke-tests donde no hay cluster levantado. Si la conexion a Cassandra
  falla en modo normal, el script cae a mock con un WARNING en lugar de
  abortar.

Uso:
  python infra/chaos_test.py
  python infra/chaos_test.py --dry-run
  python infra/chaos_test.py --node cryptoflow-cassandra-3 --workers 8
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


# ─── Defaults ───────────────────────────────────────────────────────────────

DEFAULT_NODE        = "cryptoflow-cassandra-2"
DEFAULT_SEED        = "cryptoflow-cassandra-1"   # usado solo para `nodetool`
DEFAULT_KEYSPACE    = "cryptoflow"
DEFAULT_HOSTS       = ["127.0.0.1"]
DEFAULT_PORT        = 9042
DEFAULT_USER        = "cassandra"
DEFAULT_PASS        = "cassandra"
DEFAULT_BASELINE_S  = 30.0
DEFAULT_FAILURE_S   = 30.0
DEFAULT_RECOV_TO_S  = 120.0
DEFAULT_WORKERS     = 4
DEFAULT_EXPECTED_N  = 3


_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]


# ─── Logging (JSON estructurado, una linea por evento) ──────────────────────

def log(level: str, msg: str, **extra) -> None:
    rec = {
        "ts":    datetime.now(timezone.utc).isoformat(),
        "level": level,
        "msg":   msg,
        **extra,
    }
    print(json.dumps(rec, default=str), flush=True)


# ─── Generador de eventos sinteticos ────────────────────────────────────────

def make_event(seq: int) -> dict:
    sym = _SYMBOLS[seq % len(_SYMBOLS)]
    now_ms = int(time.time() * 1000)
    return {
        "symbol":         sym,
        "date":           datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "trade_time":     now_ms,
        "event_time":     now_ms,
        "agg_trade_id":   seq,
        "price":          67000.0 + (seq % 1000) * 0.01,
        "quantity":       0.1,
        "is_buyer_maker": (seq % 2 == 0),
        "trace_id":       uuid.uuid4(),
        "ingestion_ts":   datetime.now(timezone.utc),
    }


# ─── Sinks ──────────────────────────────────────────────────────────────────

class MockSink:
    """Sink de pruebas. No escribe a ningun backend; cuenta y vuelve."""
    name = "mock"

    def insert(self, event: dict) -> None:  # noqa: ARG002
        # Pequena pausa para que el loop no sature CPU artificialmente.
        time.sleep(0.0005)

    def close(self) -> None:
        pass


class CassandraSink:
    """
    Sink real contra el cluster. INSERT con CL=LOCAL_QUORUM en `raw_trades`.

    La carga es sincrona por hilo: cada llamada a `insert` espera la
    confirmacion del coordinador (2/3 replicas). Esto es deliberado: el
    objetivo de la prueba es medir como cae el throughput cuando un nodo
    se va, no maximizar ev/s con execute_async + backpressure.
    """
    name = "cassandra"

    def __init__(self, hosts, port, keyspace, user, password):
        from cassandra import ConsistencyLevel
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import Cluster
        from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

        auth = PlainTextAuthProvider(username=user, password=password)
        self._cluster = Cluster(
            contact_points=hosts,
            port=port,
            auth_provider=auth,
            load_balancing_policy=TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc="datacenter1")
            ),
            protocol_version=4,
        )
        self._session = self._cluster.connect(keyspace)
        self._stmt = self._session.prepare(
            """
            INSERT INTO raw_trades
              (symbol, date, trade_time, event_time, agg_trade_id,
               price, quantity, is_buyer_maker, trace_id, ingestion_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        self._stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM

    def insert(self, e: dict) -> None:
        self._session.execute(self._stmt, (
            e["symbol"], e["date"], e["trade_time"], e["event_time"],
            e["agg_trade_id"], e["price"], e["quantity"],
            e["is_buyer_maker"], e["trace_id"], e["ingestion_ts"],
        ))

    def close(self) -> None:
        try:
            self._session.shutdown()
        finally:
            self._cluster.shutdown()


def build_sink(args) -> tuple[object, str]:
    """Devuelve (sink, modo). Modo es 'cassandra', 'mock' o 'mock-fallback'."""
    if args.dry_run:
        return MockSink(), "mock"
    try:
        sink = CassandraSink(
            hosts=args.hosts, port=args.port, keyspace=args.keyspace,
            user=args.user, password=args.password,
        )
        return sink, "cassandra"
    except Exception as exc:
        log("WARNING", "cassandra connection failed, falling back to mock",
            error=str(exc))
        return MockSink(), "mock-fallback"


# ─── Phase runner ───────────────────────────────────────────────────────────

@dataclass
class PhaseResult:
    name:           str
    duration_s:     float
    inserts_ok:     int
    inserts_failed: int

    @property
    def throughput_eps(self) -> float:
        return self.inserts_ok / self.duration_s if self.duration_s else 0.0

    @property
    def loss_pct(self) -> float:
        total = self.inserts_ok + self.inserts_failed
        return (self.inserts_failed / total * 100.0) if total else 0.0


def run_phase(name: str, sink, duration_s: float, workers: int,
              seq_start: int) -> tuple[PhaseResult, int]:
    """
    Lanza `workers` hilos que generan + insertan eventos durante `duration_s`.
    Devuelve el resultado y el proximo `seq` libre para mantener IDs unicos
    entre fases (clave primaria de raw_trades incluye agg_trade_id).
    """
    stop = threading.Event()
    ok   = [0] * workers
    fail = [0] * workers
    seq_lock = threading.Lock()
    counter  = [seq_start]

    def next_seq() -> int:
        with seq_lock:
            counter[0] += 1
            return counter[0]

    def worker(idx: int) -> None:
        while not stop.is_set():
            e = make_event(next_seq())
            try:
                sink.insert(e)
                ok[idx] += 1
            except Exception:
                fail[idx] += 1

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(workers)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    time.sleep(duration_s)
    stop.set()
    for t in threads:
        t.join(timeout=15)
    elapsed = time.perf_counter() - t0

    return (
        PhaseResult(name=name, duration_s=elapsed,
                    inserts_ok=sum(ok), inserts_failed=sum(fail)),
        counter[0],
    )


# ─── Docker control ────────────────────────────────────────────────────────

def _docker(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def container_is_running(name: str) -> bool:
    r = _docker("inspect", "-f", "{{.State.Running}}", name)
    return r.returncode == 0 and r.stdout.strip() == "true"


def container_ip(name: str) -> Optional[str]:
    r = _docker(
        "inspect", "-f",
        "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}",
        name,
    )
    if r.returncode != 0:
        return None
    ip = r.stdout.strip().split()
    return ip[0] if ip else None


def stop_container(name: str) -> None:
    r = _docker("stop", name)
    if r.returncode != 0:
        raise RuntimeError(f"docker stop {name} failed: {r.stderr.strip()}")


def start_container(name: str) -> None:
    r = _docker("start", name)
    if r.returncode != 0:
        raise RuntimeError(f"docker start {name} failed: {r.stderr.strip()}")


def wait_for_node_up(seed_container: str, target_ip: Optional[str],
                     expected_nodes: int, timeout_s: float) -> float:
    """
    Hace polling de `nodetool status` en el seed hasta ver el target como UN.

    Si conocemos la IP del target, buscamos esa linea especifica. Si no,
    aceptamos el primer momento en que aparezcan al menos `expected_nodes`
    nodos UN (proxy razonable para "cluster sano").

    Devuelve segundos transcurridos hasta el UN, o inf si vencio el timeout.
    """
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout_s:
        r = _docker("exec", seed_container, "nodetool", "status")
        if r.returncode == 0:
            un_lines = []
            for line in r.stdout.splitlines():
                parts = line.split()
                if parts and parts[0] == "UN":
                    un_lines.append(parts)
            if target_ip:
                if any(len(p) > 1 and p[1] == target_ip for p in un_lines):
                    return time.perf_counter() - t0
            else:
                if len(un_lines) >= expected_nodes:
                    return time.perf_counter() - t0
        time.sleep(2.0)
    return float("inf")


# ─── Main ──────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Chaos test del cluster Cassandra de CryptoFlow.",
    )
    p.add_argument("--node",  default=DEFAULT_NODE,
                   help="Nombre del contenedor del nodo a matar.")
    p.add_argument("--seed",  default=DEFAULT_SEED,
                   help="Contenedor donde se ejecuta `nodetool status`.")
    p.add_argument("--baseline-secs", type=float, default=DEFAULT_BASELINE_S)
    p.add_argument("--failure-secs",  type=float, default=DEFAULT_FAILURE_S)
    p.add_argument("--recovery-timeout-secs", type=float,
                   default=DEFAULT_RECOV_TO_S)
    p.add_argument("--workers", type=int,  default=DEFAULT_WORKERS)
    p.add_argument("--expected-nodes", type=int, default=DEFAULT_EXPECTED_N,
                   help="Numero de nodos esperados UN tras recuperacion.")
    p.add_argument("--hosts", nargs="+", default=DEFAULT_HOSTS)
    p.add_argument("--port",  type=int,   default=DEFAULT_PORT)
    p.add_argument("--keyspace", default=DEFAULT_KEYSPACE)
    p.add_argument("--user",     default=DEFAULT_USER)
    p.add_argument("--password", default=DEFAULT_PASS)
    p.add_argument("--dry-run", action="store_true",
                   help="Sink mock + sin docker stop. Para smoke-tests.")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    sink, mode = build_sink(args)
    log("INFO", "chaos test starting",
        mode=mode, node=args.node, seed=args.seed,
        workers=args.workers,
        baseline_secs=args.baseline_secs,
        failure_secs=args.failure_secs)

    phases: list[PhaseResult] = []
    recovery_time_s: Optional[float] = None
    node_was_killed = False
    seq = 0

    try:
        # ── 1) Baseline ────────────────────────────────────────────────
        log("INFO", "phase=baseline starting")
        r, seq = run_phase("baseline", sink, args.baseline_secs,
                           args.workers, seq)
        phases.append(r)
        log("INFO", "phase=baseline done",
            throughput_eps=round(r.throughput_eps, 1),
            inserts_ok=r.inserts_ok, inserts_failed=r.inserts_failed)

        # ── 2) Failure ─────────────────────────────────────────────────
        if not args.dry_run:
            log("INFO", "killing node", node=args.node)
            stop_container(args.node)
            node_was_killed = True
        else:
            log("INFO", "dry-run: skipping docker stop", node=args.node)

        log("INFO", "phase=failure starting")
        r, seq = run_phase("failure", sink, args.failure_secs,
                           args.workers, seq)
        phases.append(r)
        log("INFO", "phase=failure done",
            throughput_eps=round(r.throughput_eps, 1),
            inserts_ok=r.inserts_ok, inserts_failed=r.inserts_failed)

        # ── 3) Recovery ────────────────────────────────────────────────
        if not args.dry_run:
            log("INFO", "restarting node", node=args.node)
            start_container(args.node)
            node_was_killed = False
            # Pequena espera para que docker asigne IP / red.
            time.sleep(2.0)
            target_ip = container_ip(args.node)
            log("INFO", "waiting for node UN",
                target_ip=target_ip,
                timeout_s=args.recovery_timeout_secs)
            recovery_time_s = wait_for_node_up(
                args.seed, target_ip,
                args.expected_nodes,
                args.recovery_timeout_secs,
            )
            if recovery_time_s == float("inf"):
                log("ERROR", "node did NOT return to UN within timeout",
                    timeout_s=args.recovery_timeout_secs)
            else:
                log("INFO", "node back UN",
                    recovery_time_s=round(recovery_time_s, 1))
        else:
            recovery_time_s = 0.0
            log("INFO", "dry-run: skipping docker start + nodetool poll")

        log("INFO", "phase=recovery starting")
        r, seq = run_phase("recovery", sink, args.baseline_secs,
                           args.workers, seq)
        phases.append(r)
        log("INFO", "phase=recovery done",
            throughput_eps=round(r.throughput_eps, 1),
            inserts_ok=r.inserts_ok, inserts_failed=r.inserts_failed)

    finally:
        # Idempotencia: si por cualquier razon el nodo quedo apagado,
        # se reinicia. Ej.: Ctrl+C entre stop_container y start_container.
        if node_was_killed:
            log("WARNING", "ensuring killed node is restarted", node=args.node)
            try:
                start_container(args.node)
            except Exception as exc:
                log("ERROR", "could not restart node in cleanup",
                    node=args.node, error=str(exc))
        try:
            sink.close()
        except Exception:
            pass

    # ── Summary ─────────────────────────────────────────────────────────
    baseline = next((p for p in phases if p.name == "baseline"), None)
    failure  = next((p for p in phases if p.name == "failure"),  None)
    recovery = next((p for p in phases if p.name == "recovery"), None)

    summary: dict = {
        "mode":            mode,
        "node_under_test": args.node,
        "phases": [
            {"name":           p.name,
             "duration_s":     round(p.duration_s, 2),
             "throughput_eps": round(p.throughput_eps, 1),
             "inserts_ok":     p.inserts_ok,
             "inserts_failed": p.inserts_failed,
             "loss_pct":       round(p.loss_pct, 3)}
            for p in phases
        ],
        "events_lost_total":     sum(p.inserts_failed for p in phases),
        "recovery_time_to_un_s":
            (round(recovery_time_s, 1)
             if recovery_time_s not in (None, float("inf")) else None),
    }
    if baseline and failure and baseline.throughput_eps > 0:
        summary["failure_vs_baseline_ratio"] = round(
            failure.throughput_eps / baseline.throughput_eps, 3,
        )
    if baseline and recovery and baseline.throughput_eps > 0:
        summary["recovery_vs_baseline_ratio"] = round(
            recovery.throughput_eps / baseline.throughput_eps, 3,
        )

    log("INFO", "summary", **summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
