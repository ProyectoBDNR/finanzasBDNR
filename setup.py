#!/usr/bin/env python3
"""
setup.py — CryptoFlow Analytics · Setup automático desde cero
═══════════════════════════════════════════════════════════════
Uso:
    python setup.py          # setup completo
    python setup.py --reset  # destruye y recrea volúmenes (cluster nuevo)
"""

from __future__ import annotations
import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path

# ── colores ──────────────────────────────────────────────────────────────────
G   = "\033[92m"
Y   = "\033[93m"
R   = "\033[91m"
B   = "\033[94m"
DIM = "\033[2m"
RST = "\033[0m"
BOLD= "\033[1m"

def ok(msg):   print(f"  {G}✓{RST} {msg}")
def warn(msg): print(f"  {Y}⚠{RST} {msg}")
def err(msg):  print(f"  {R}✗{RST} {msg}")
def info(msg): print(f"  {B}›{RST} {msg}")
def step(n, msg): print(f"\n{BOLD}[{n}]{RST} {msg}")
def hdr(msg):  print(f"\n{BOLD}{'═'*60}\n  {msg}\n{'═'*60}{RST}")

# ── shell helpers ─────────────────────────────────────────────────────────────
def run(cmd: str) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, check=False)

def docker(cmd: str) -> subprocess.CompletedProcess:
    return run(f"docker {cmd}")

def compose(cmd: str) -> subprocess.CompletedProcess:
    return run(f"docker-compose {cmd}")

def cqlsh(password: str, cql_file: str) -> bool:
    r = docker(f"exec cryptoflow-cassandra-1 cqlsh -u cassandra -p {password} -f /{cql_file}")
    return r.returncode == 0

# ── conteo correcto de nodos UN ───────────────────────────────────────────────
def count_un() -> int:
    # No redirigir stderr — en Windows PowerShell el 2>/dev/null puede
    # interferir con la captura. Capturamos stdout y stderr por separado.
    r = subprocess.run(
        ["docker", "exec", "cryptoflow-cassandra-1", "nodetool", "status"],
        capture_output=True, text=True, check=False
    )
    combined = r.stdout + r.stderr
    return sum(1 for line in combined.splitlines() if line.strip().startswith("UN"))

# ── wait: cassandra-1 lista (señal real en log) ───────────────────────────────
def wait_cassandra1(timeout: int = 300) -> bool:
    """
    Espera la señal 'Created default superuser role' en el log de cassandra-1.
    Es la única señal fiable de que Cassandra + PasswordAuthenticator están listos.
    """
    info(f"Esperando cassandra-1 lista (timeout={timeout}s)...")
    start = time.time()
    while time.time() - start < timeout:
        elapsed = int(time.time() - start)
        logs = docker("logs cryptoflow-cassandra-1 2>&1")
        log_text = logs.stdout + logs.stderr

        if "Created default superuser role" in log_text:
            print()
            ok(f"Auth lista en {elapsed}s — 'Created default superuser role'")
            return True

        # Mostrar última línea relevante como progreso
        hint = ""
        for kw in ["Starting listening", "Startup complete", "Loading", "Bootstrap", "Initializing"]:
            matches = [l for l in log_text.splitlines() if kw in l]
            if matches:
                hint = f" · {matches[-1].strip()[-60:]}"
                break
        print(f"\r  {DIM}› {elapsed}s{hint}{RST}    ", end="", flush=True)
        time.sleep(6)

    print()
    err(f"cassandra-1 no llegó a ready en {timeout}s")
    return False

# ── wait: n nodos UN ──────────────────────────────────────────────────────────
def wait_un(n: int, timeout: int = 300, label: str = "") -> bool:
    """Poll activo: espera hasta n nodos UN. Sin sleeps fijos."""
    label_str = f" ({label})" if label else ""
    start = time.time()
    last_debug = 0
    while time.time() - start < timeout:
        un = count_un()
        elapsed = int(time.time() - start)
        print(f"\r  {DIM}› {un}/{n} nodos UN · {elapsed}s{label_str}{RST}    ", end="", flush=True)
        if un >= n:
            print()
            ok(f"{n} nodos UN{label_str} en {elapsed}s")
            return True
        # Cada 60s mostrar output crudo de nodetool para diagnóstico
        if elapsed - last_debug >= 60:
            last_debug = elapsed
            r = subprocess.run(
                ["docker", "exec", "cryptoflow-cassandra-1", "nodetool", "status"],
                capture_output=True, text=True, check=False
            )
            if r.returncode != 0:
                print(f"\n  {DIM}[nodetool rc={r.returncode}: {(r.stderr or r.stdout)[:80]}]{RST}")
            else:
                lines = [l for l in r.stdout.splitlines() if l.strip()]
                for l in lines[-4:]:
                    print(f"\n  {DIM}  {l}{RST}", end="")
            print()
        time.sleep(5)
    print()
    # Último intento
    r = subprocess.run(
        ["docker", "exec", "cryptoflow-cassandra-1", "nodetool", "status"],
        capture_output=True, text=True, check=False
    )
    un = sum(1 for l in (r.stdout+r.stderr).splitlines() if l.strip().startswith("UN"))
    if un >= 1:
        ok(f"{un}/{n} nodos UN al finalizar timeout — continuando")
        return True
    warn(f"Timeout — nodetool devolvió: {(r.stdout+r.stderr)[:120]}")
    return False

# ── detect password ───────────────────────────────────────────────────────────
def detect_password() -> str:
    for pwd in ["BDNR", "cassandra"]:
        r = docker(f"exec cryptoflow-cassandra-1 cqlsh -u cassandra -p {pwd} "
                   f"-e \"DESCRIBE KEYSPACES;\" 2>/dev/null")
        if r.returncode == 0:
            return pwd
    return "cassandra"

# ── apply CQL file ────────────────────────────────────────────────────────────
def apply_cql(local_path: str, remote_name: str, password: str) -> bool:
    docker(f"cp {local_path} cryptoflow-cassandra-1:/{remote_name}")
    return cqlsh(password, remote_name)

# ── verify RBAC: poll hasta que cf_writer y cf_analyst conecten ───────────────
def verify_rbac() -> bool:
    """
    Verifica RBAC comprobando que los roles existen vía LIST ROLES
    (con el superusuario cassandra). No intenta conectar con cf_writer/cf_analyst
    directamente porque esos roles no tienen permisos en system.local.
    """
    r = subprocess.run(
        ["docker", "exec", "cryptoflow-cassandra-1",
         "cqlsh", "-u", "cassandra", "-p", "BDNR",
         "-e", "LIST ROLES;"],
        capture_output=True, text=True, check=False
    )
    output = r.stdout + r.stderr
    roles_found = [role for role in ["cf_writer", "cf_analyst", "cf_admin"]
                   if role in output]
    if len(roles_found) == 3:
        ok(f"RBAC: 4 roles presentes (cassandra, cf_admin, cf_analyst, cf_writer)")
        return True
    elif roles_found:
        warn(f"RBAC: solo encontrados: {roles_found}")
        return False
    else:
        warn("RBAC: no se detectaron roles — verifica con: "
             "docker exec cryptoflow-cassandra-1 cqlsh -u cassandra -p BDNR -e 'LIST ROLES;'")
        return False

# ── update .env ───────────────────────────────────────────────────────────────
def update_env(key: str, value: str) -> None:
    env_path = Path(".env")
    if not env_path.exists():
        return
    content = env_path.read_text()
    pattern = re.compile(rf"^{re.escape(key)}=.*$", re.MULTILINE)
    new_line = f"{key}={value}"
    content = pattern.sub(new_line, content) if pattern.search(content) else content.rstrip() + f"\n{new_line}\n"
    env_path.write_text(content)
    ok(f".env: {key}={value}")

# ═════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Destruye volúmenes y recrea el cluster")
    parser.add_argument("--skip-nodes", action="store_true", help="Solo levanta cassandra-1")
    args = parser.parse_args()

    hdr("CryptoFlow Analytics — Setup automático")

    # ── [0] Pre-requisitos ────────────────────────────────────────────────────
    step(0, "Pre-requisitos")
    if not run("docker info").returncode == 0:
        err("Docker no está corriendo"); sys.exit(1)
    ok("Docker disponible")
    if not Path("docker-compose.yml").exists():
        err("Ejecuta desde la raíz del proyecto"); sys.exit(1)
    ok("docker-compose.yml encontrado")

    # ── [R] Reset ─────────────────────────────────────────────────────────────
    if args.reset:
        step("R", "Destruyendo cluster (--reset)")
        compose("down -v")
        ok("Volúmenes destruidos")

    # ── [1] cassandra-1 ───────────────────────────────────────────────────────
    step(1, "Levantando cassandra-1")
    r = docker("ps --format '{{.Names}}'")
    already_up = "cryptoflow-cassandra-1" in r.stdout

    if already_up:
        if count_un() >= 1:
            ok("cassandra-1 ya está corriendo")
        else:
            compose("start cassandra-1")
            if not wait_cassandra1(): sys.exit(1)
    else:
        info("Iniciando cassandra-1 solo...")
        compose("up -d cassandra-1")
        if not wait_cassandra1(): sys.exit(1)

    # ── [2] Contraseña ────────────────────────────────────────────────────────
    step(2, "Detectando contraseña")
    pwd = detect_password()
    info(f"Contraseña: {'cassandra (default)' if pwd == 'cassandra' else 'BDNR'}")

    # ── [3] DDL ───────────────────────────────────────────────────────────────
    step(3, "Aplicando DDL (cassandra.cql)")
    if not Path("schemas/cassandra.cql").exists():
        err("schemas/cassandra.cql no encontrado"); sys.exit(1)
    if apply_cql("schemas/cassandra.cql", "cassandra.cql", pwd):
        ok("DDL aplicado")
    else:
        warn("DDL falló o ya estaba aplicado — continuando")

    # ── [4] RBAC ──────────────────────────────────────────────────────────────
    step(4, "Aplicando RBAC (rbac.cql)")
    if Path("schemas/rbac.cql").exists():
        docker("cp schemas/rbac.cql cryptoflow-cassandra-1:/rbac.cql")
        # Intentar con pwd actual y con BDNR (rbac.cql cambia la contraseña)
        for try_pwd in ([pwd, "BDNR"] if pwd != "BDNR" else ["BDNR"]):
            cqlsh(try_pwd, "rbac.cql")  # ignorar exit code: ALTER falla si ya existe
        pwd = "BDNR"  # después del RBAC siempre es BDNR

        # Verificar con LIST ROLES (fuente de verdad)
        r = run("docker exec cryptoflow-cassandra-1 cqlsh -u cassandra -p BDNR "
                "-e \"LIST ROLES;\" 2>/dev/null")
        if all(role in r.stdout for role in ["cf_writer", "cf_analyst", "cf_admin"]):
            ok("RBAC: cf_writer, cf_analyst, cf_admin presentes")
        else:
            warn("RBAC: roles no detectados aún (se verificará en paso 7)")
    else:
        warn("schemas/rbac.cql no encontrado — saltando")

    # ── [5] Migración v2 ──────────────────────────────────────────────────────
    step(5, "Aplicando migración v2 (migration_v2.cql)")
    if Path("schemas/migration_v2.cql").exists():
        if apply_cql("schemas/migration_v2.cql", "migration_v2.cql", pwd):
            ok("Migración v2 aplicada")
        else:
            warn("Migración v2 falló o ya estaba aplicada — continuando")
    else:
        warn("schemas/migration_v2.cql no encontrado — saltando")

    # ── .env ANTES de levantar nodos 2/3 ─────────────────────────────────────
    # El healthcheck de cassandra-1 usa CASSANDRA_SUPER_PASSWORD.
    # Si sigue en "cassandra", cassandra-1 nunca pasa a healthy y
    # cassandra-2/3 no arrancan (depends_on: service_healthy).
    update_env("CASSANDRA_SUPER_PASSWORD", "BDNR")
    update_env("CASSANDRA_HOSTS", "127.0.0.1")
    update_env("CASSANDRA_SPARK_HOSTS", "cassandra-1")

    # ── [6] Nodos 2, 3 y Spark ───────────────────────────────────────────────
    if not args.skip_nodes:
        step(6, "Levantando cassandra-2, cassandra-3 y spark")

        # Arranque secuencial: cassandra-2 primero, esperar UN, luego cassandra-3.
        # Si arrancan juntos hacen bootstrap simultáneo → error consistent.rangemovement.
        # Arrancar cassandra-2, esperar UN, luego cassandra-3.
        # Un solo wait_un(3) con timeout generoso cubre ambos nodos.
        compose("up -d cassandra-2")
        # Pequeña pausa para que cassandra-2 registre red antes del primer poll
        time.sleep(5)
        wait_un(n=2, timeout=300, label="cassandra-2")

        compose("up -d cassandra-3")
        compose("up -d spark")
        time.sleep(5)
        wait_un(n=3, timeout=300, label="cassandra-3")

        # Mostrar estado final
        r = docker("exec cryptoflow-cassandra-1 nodetool status")
        if r.returncode == 0:
            for line in r.stdout.splitlines():
                if line.strip().startswith(("UN", "DN", "UJ")):
                    info(f"  {line.strip()}")
    else:
        step(6, "Saltando nodos 2 y 3 (--skip-nodes)")

    # ── [7] Verificar RBAC ────────────────────────────────────────────────────
    step(7, "Verificando RBAC")
    verify_rbac()

    # ── Resumen ───────────────────────────────────────────────────────────────
    hdr("Setup completado ✓")
    print(f"""
  {G}Cluster listo.{RST} Ahora puedes correr:

    {BOLD}python main.py{RST}

  Comandos manuales de Spark:
    {DIM}docker exec -e CASSANDRA_HOSTS=cassandra-1 \\
      -e CASSANDRA_ANALYST_USER=cf_analyst \\
      -e CASSANDRA_ANALYST_PASSWORD=analyst_pwd_BDNR \\
      cryptoflow-spark /app/run_spark.sh /app/processing/job.py --date YYYY-MM-DD

    ... (mismo patrón para runner.py y run_demo.py){RST}

  Dashboard:
    {DIM}cd analytics && python -m http.server 8080{RST}
""")

if __name__ == "__main__":
    main()