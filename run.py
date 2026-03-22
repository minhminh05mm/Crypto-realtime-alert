from __future__ import annotations

import argparse
import json
import os
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent
ENV_FILE = ROOT_DIR / ".env"
DOCKER_COMPOSE_FILE = ROOT_DIR / "infrastructure" / "docker-compose.yml"
VENV_PYTHON = ROOT_DIR / ".venv" / "bin" / "python"
RUNTIME_DIR = ROOT_DIR / ".runtime"
LOG_DIR = RUNTIME_DIR / "logs"
PID_FILE = RUNTIME_DIR / "processes.json"


@dataclass(frozen=True, slots=True)
class ManagedProcess:
    name: str
    module: str
    workdir: Path


@dataclass(slots=True)
class ProcessRuntime:
    name: str
    module: str
    pid: int
    pgid: int
    workdir: str
    log_path: str
    started_at: str


MANAGED_PROCESSES: tuple[ManagedProcess, ...] = (
    ManagedProcess(
        name="news_api_client",
        module="src.news_api_client",
        workdir=ROOT_DIR / "data_ingestion",
    ),
    ManagedProcess(
        name="price_ws_client",
        module="src.price_ws_client",
        workdir=ROOT_DIR / "data_ingestion",
    ),
    ManagedProcess(
        name="spark_pipeline",
        module="src.spark_pipeline",
        workdir=ROOT_DIR / "stream_processing",
    ),
)


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        if args.command == "start":
            return start_command()
        if args.command == "status":
            return status_command()
        if args.command == "stop":
            return stop_command(with_infra=args.with_infra)
        if args.command == "logs":
            return logs_command(process_name=args.process, lines=args.lines)
    except KeyboardInterrupt:
        print("Interrupted by user.")
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1

    parser.print_help()
    return 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Orchestrate the crypto realtime alert system from one entrypoint."
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser(
        "start",
        help="Start Docker infrastructure, ensure topics/table, and run all Python services.",
    )
    subparsers.add_parser(
        "status",
        help="Check infrastructure, managed processes, Kafka topics, Redis keys, and ClickHouse rows.",
    )

    stop_parser = subparsers.add_parser(
        "stop",
        help="Stop managed Python services. Use --with-infra to stop Docker infrastructure too.",
    )
    stop_parser.add_argument(
        "--with-infra",
        action="store_true",
        help="Also stop the Docker infrastructure after stopping Python services.",
    )

    logs_parser = subparsers.add_parser(
        "logs",
        help="Show the latest log lines for a managed process.",
    )
    logs_parser.add_argument(
        "process",
        choices=[process.name for process in MANAGED_PROCESSES] + ["all"],
        help="Managed process name or 'all'.",
    )
    logs_parser.add_argument(
        "--lines",
        type=int,
        default=30,
        help="Number of last log lines to show.",
    )

    parser.set_defaults(command="start")
    return parser


def start_command() -> int:
    _ensure_local_prerequisites()
    env = _load_env_file()
    _ensure_runtime_dirs()

    print("Starting Docker infrastructure...")
    _run_command(_docker_compose_command("up", "-d"))

    print("Waiting for infrastructure to become healthy...")
    _wait_for_infrastructure(env, timeout_seconds=180)

    print("Ensuring Kafka topics exist...")
    _ensure_kafka_topics(env)

    print("Ensuring ClickHouse database and alerts table exist...")
    _ensure_clickhouse_schema(env)

    state = _load_state()
    for process in MANAGED_PROCESSES:
        state = _start_managed_process(process, state)

    _save_state(state)

    print()
    print("System started successfully.")
    print("Use 'python run.py status' to verify health and data flow.")
    print("Use 'python run.py logs all --lines 40' to inspect recent logs.")
    return 0


def status_command() -> int:
    _ensure_runtime_dirs()
    env = _load_env_file()

    infra_status = _collect_infrastructure_status(env)
    process_status = _collect_process_status()
    kafka_topics = _collect_kafka_topics(env)
    redis_summary = _collect_redis_summary(env)
    clickhouse_summary = _collect_clickhouse_summary(env)

    print("SYSTEM STATUS")
    print("=" * 72)
    _print_infrastructure_status(infra_status)
    _print_process_status(process_status)
    _print_kafka_status(kafka_topics, env)
    _print_redis_status(redis_summary)
    _print_clickhouse_status(clickhouse_summary)
    _print_health_summary(infra_status, process_status, redis_summary, clickhouse_summary)

    problems = [
        item
        for item in infra_status
        if item["severity"] == "error"
    ] + [
        item
        for item in process_status
        if item["status"] != "running"
    ]
    return 1 if problems else 0


def stop_command(with_infra: bool) -> int:
    _ensure_runtime_dirs()
    state = _load_state()

    for process in reversed(MANAGED_PROCESSES):
        runtime = state.get(process.name)
        if runtime is None:
            continue
        _stop_runtime(runtime)
        state.pop(process.name, None)

    _save_state(state)

    if with_infra:
        print("Stopping Docker infrastructure...")
        _run_command(_docker_compose_command("down"))

    print("System stopped successfully.")
    return 0


def logs_command(process_name: str, lines: int) -> int:
    _ensure_runtime_dirs()

    targets = list(MANAGED_PROCESSES)
    if process_name != "all":
        targets = [process for process in MANAGED_PROCESSES if process.name == process_name]

    for process in targets:
        log_path = LOG_DIR / f"{process.name}.log"
        print(f"===== {process.name} | {log_path} =====")
        if not log_path.exists():
            print("Log file not found.")
            continue
        for line in _tail_file(log_path, lines):
            print(line)
        print()

    return 0


def _ensure_local_prerequisites() -> None:
    if not ENV_FILE.exists():
        raise FileNotFoundError(f"Missing environment file: {ENV_FILE}")

    if not DOCKER_COMPOSE_FILE.exists():
        raise FileNotFoundError(f"Missing Docker Compose file: {DOCKER_COMPOSE_FILE}")

    if not VENV_PYTHON.exists():
        raise FileNotFoundError(
            f"Missing project virtualenv interpreter: {VENV_PYTHON}. "
            "Create it first with 'python3 -m venv .venv' and install requirements."
        )

    _run_command(["docker", "version"], capture_output=True)
    _run_command(["docker", "compose", "version"], capture_output=True)


def _ensure_runtime_dirs() -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def _load_env_file() -> dict[str, str]:
    env: dict[str, str] = {}
    for raw_line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip("'").strip('"')
    return env


def _docker_compose_command(*args: str) -> list[str]:
    return [
        "docker",
        "compose",
        "--env-file",
        str(ENV_FILE),
        "-f",
        str(DOCKER_COMPOSE_FILE),
        *args,
    ]


def _run_command(
    command: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    capture_output: bool = False,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=False,
        capture_output=capture_output,
        text=True,
    )
    if check and result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        message = stderr or stdout or "Command failed."
        raise RuntimeError(f"{' '.join(shlex.quote(part) for part in command)} -> {message}")
    return result


def _wait_for_infrastructure(env: dict[str, str], timeout_seconds: int) -> None:
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        statuses = _collect_infrastructure_status(env)
        if all(item["healthy"] for item in statuses if item["required"]):
            return
        time.sleep(5)
    raise TimeoutError("Infrastructure did not become healthy within the expected time window.")


def _collect_infrastructure_status(env: dict[str, str]) -> list[dict[str, Any]]:
    project_name = env.get("COMPOSE_PROJECT_NAME", "crypto-realtime-alert")
    containers = [
        {"name": f"{project_name}-zookeeper", "required": True},
        {"name": f"{project_name}-kafka", "required": True},
        {"name": f"{project_name}-redis", "required": True},
        {"name": f"{project_name}-clickhouse", "required": True},
        {"name": f"{project_name}-kafka-init", "required": False},
    ]
    statuses: list[dict[str, Any]] = []

    for container in containers:
        inspect_result = _run_command(
            [
                "docker",
                "inspect",
                "--format",
                "{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}|{{.State.ExitCode}}",
                container["name"],
            ],
            capture_output=True,
            check=False,
        )
        if inspect_result.returncode != 0:
            statuses.append(
                {
                    "name": container["name"],
                    "required": container["required"],
                    "healthy": False,
                    "severity": "error" if container["required"] else "warning",
                    "message": "container not found",
                }
            )
            continue

        raw_status = inspect_result.stdout.strip()
        state, health, exit_code = (raw_status.split("|", 2) + ["", ""])[:3]
        healthy = state == "running" and (not health or health == "healthy")
        severity = "info" if healthy else ("error" if container["required"] else "warning")

        if container["name"].endswith("kafka-init"):
            healthy = state in {"running", "exited"} and exit_code == "0"
            severity = "info" if healthy else "warning"

        statuses.append(
            {
                "name": container["name"],
                "required": container["required"],
                "healthy": healthy,
                "severity": severity,
                "message": f"state={state or 'unknown'} health={health or 'n/a'} exit_code={exit_code or 'n/a'}",
            }
        )

    return statuses


def _ensure_kafka_topics(env: dict[str, str]) -> None:
    kafka_container = _container_name(env, "kafka")
    bootstrap_server = env.get("KAFKA_INTERNAL_BROKER", "kafka:29092")
    topics = [
        env["KAFKA_TOPIC_RAW_PRICES"],
        env["KAFKA_TOPIC_RAW_NEWS"],
    ]

    for topic in topics:
        _run_command(
            [
                "docker",
                "exec",
                kafka_container,
                "kafka-topics",
                "--bootstrap-server",
                bootstrap_server,
                "--create",
                "--if-not-exists",
                "--topic",
                topic,
                "--partitions",
                env.get("KAFKA_TOPIC_PARTITIONS", "3"),
                "--replication-factor",
                env.get("KAFKA_TOPIC_REPLICATION_FACTOR", "1"),
            ]
        )


def _ensure_clickhouse_schema(env: dict[str, str]) -> None:
    clickhouse_container = _container_name(env, "clickhouse")
    database_name = env["CLICKHOUSE_DB"]
    table_name = env["CLICKHOUSE_ALERTS_TABLE"]
    user = env["CLICKHOUSE_USER"]
    password = env["CLICKHOUSE_PASSWORD"]

    create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
    (
        `timestamp` DateTime64(3, 'UTC'),
        `symbol` String,
        `current_price` Float64,
        `first_price` Float64,
        `min_price` Float64,
        `max_price` Float64,
        `price_change_pct` Float64,
        `price_tick_count` UInt64,
        `news_count` UInt64,
        `sentiment_label` LowCardinality(String),
        `sentiment_score` Float64,
        `alert_status` LowCardinality(String),
        `headline` String,
        `window_start` DateTime64(3, 'UTC'),
        `window_end` DateTime64(3, 'UTC'),
        `processed_at` DateTime64(3, 'UTC')
    )
    ENGINE = ReplacingMergeTree(processed_at)
    ORDER BY (`timestamp`, `symbol`)
    """

    for query in (create_database_query, create_table_query):
        _run_command(
            [
                "docker",
                "exec",
                clickhouse_container,
                "clickhouse-client",
                "--user",
                user,
                "--password",
                password,
                "--query",
                query,
            ]
        )


def _start_managed_process(
    process: ManagedProcess,
    current_state: dict[str, ProcessRuntime],
) -> dict[str, ProcessRuntime]:
    existing = current_state.get(process.name)
    if existing is not None and _is_pid_running(existing.pid):
        print(f"Process already running: {process.name} pid={existing.pid}")
        return current_state

    log_path = LOG_DIR / f"{process.name}.log"
    child_env = os.environ.copy()
    child_env.update(_load_env_file())
    child_env["PYTHONPATH"] = _build_pythonpath(process.workdir, child_env.get("PYTHONPATH"))
    child_env["PYTHONUNBUFFERED"] = "1"

    with log_path.open("a", encoding="utf-8") as log_file:
        log_file.write(
            f"\n[{_utc_now()}] starting {process.name} with module {process.module}\n"
        )
        log_file.flush()
        popen = subprocess.Popen(
            [str(VENV_PYTHON), "-m", process.module],
            cwd=process.workdir,
            env=child_env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            text=True,
        )

    time.sleep(3)
    if popen.poll() is not None:
        last_lines = "\n".join(_tail_file(log_path, 20))
        raise RuntimeError(
            f"Managed process '{process.name}' exited immediately.\nRecent log output:\n{last_lines}"
        )

    runtime = ProcessRuntime(
        name=process.name,
        module=process.module,
        pid=popen.pid,
        pgid=popen.pid,
        workdir=str(process.workdir),
        log_path=str(log_path),
        started_at=_utc_now(),
    )
    current_state[process.name] = runtime
    print(f"Started {process.name} pid={popen.pid}")
    return current_state


def _collect_process_status() -> list[dict[str, Any]]:
    state = _load_state()
    status_items: list[dict[str, Any]] = []

    for process in MANAGED_PROCESSES:
        runtime = state.get(process.name)
        log_path = LOG_DIR / f"{process.name}.log"
        if runtime is None:
            status_items.append(
                {
                    "name": process.name,
                    "status": "stopped",
                    "message": "not started by run.py",
                    "log_path": str(log_path),
                }
            )
            continue

        running = _is_pid_running(runtime.pid)
        message = f"pid={runtime.pid} started_at={runtime.started_at}"
        if log_path.exists():
            modified_at = datetime.fromtimestamp(log_path.stat().st_mtime, UTC)
            message += f" last_log_at={modified_at.isoformat()}"

        status_items.append(
            {
                "name": process.name,
                "status": "running" if running else "stopped",
                "message": message,
                "log_path": runtime.log_path,
            }
        )

    return status_items


def _collect_kafka_topics(env: dict[str, str]) -> dict[str, Any]:
    kafka_container = _container_name(env, "kafka")
    bootstrap_server = env.get("KAFKA_INTERNAL_BROKER", "kafka:29092")
    result = _run_command(
        [
            "docker",
            "exec",
            kafka_container,
            "kafka-topics",
            "--bootstrap-server",
            bootstrap_server,
            "--list",
        ],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return {"ok": False, "topics": [], "message": result.stderr.strip() or result.stdout.strip()}

    topics = sorted(line.strip() for line in result.stdout.splitlines() if line.strip())
    return {"ok": True, "topics": topics, "message": f"topics={len(topics)}"}


def _collect_redis_summary(env: dict[str, str]) -> dict[str, Any]:
    redis_container = _container_name(env, "redis")
    prefix = f"{env['REDIS_ALERT_KEY_PREFIX']}*"

    size_result = _run_command(
        ["docker", "exec", redis_container, "redis-cli", "--raw", "DBSIZE"],
        capture_output=True,
        check=False,
    )
    keys_result = _run_command(
        ["docker", "exec", redis_container, "redis-cli", "--raw", "KEYS", prefix],
        capture_output=True,
        check=False,
    )

    if size_result.returncode != 0 or keys_result.returncode != 0:
        return {
            "ok": False,
            "dbsize": None,
            "keys": [],
            "message": size_result.stderr.strip() or keys_result.stderr.strip() or "Redis check failed.",
        }

    keys = [line.strip() for line in keys_result.stdout.splitlines() if line.strip()]
    return {
        "ok": True,
        "dbsize": int(size_result.stdout.strip() or "0"),
        "keys": keys,
        "message": f"alert_keys={len(keys)}",
    }


def _collect_clickhouse_summary(env: dict[str, str]) -> dict[str, Any]:
    clickhouse_container = _container_name(env, "clickhouse")
    query = (
        f"SELECT count() FROM {env['CLICKHOUSE_DB']}.{env['CLICKHOUSE_ALERTS_TABLE']}"
    )
    result = _run_command(
        [
            "docker",
            "exec",
            clickhouse_container,
            "clickhouse-client",
            "--user",
            env["CLICKHOUSE_USER"],
            "--password",
            env["CLICKHOUSE_PASSWORD"],
            "--query",
            query,
        ],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return {"ok": False, "row_count": None, "message": result.stderr.strip() or result.stdout.strip()}

    return {
        "ok": True,
        "row_count": int(result.stdout.strip() or "0"),
        "message": "clickhouse query succeeded",
    }


def _print_infrastructure_status(status_items: list[dict[str, Any]]) -> None:
    print("Infrastructure")
    for item in status_items:
        marker = "OK" if item["healthy"] else "FAIL"
        print(f"- {marker:<4} {item['name']}: {item['message']}")
    print()


def _print_process_status(status_items: list[dict[str, Any]]) -> None:
    print("Managed Processes")
    for item in status_items:
        marker = "OK" if item["status"] == "running" else "FAIL"
        print(f"- {marker:<4} {item['name']}: {item['message']}")
    print()


def _print_kafka_status(summary: dict[str, Any], env: dict[str, str]) -> None:
    expected_topics = {env["KAFKA_TOPIC_RAW_PRICES"], env["KAFKA_TOPIC_RAW_NEWS"]}
    existing_topics = set(summary.get("topics", []))
    print("Kafka")
    if not summary["ok"]:
        print(f"- FAIL topic check: {summary['message']}")
    else:
        missing = sorted(expected_topics - existing_topics)
        marker = "OK" if not missing else "FAIL"
        print(f"- {marker:<4} topics present: {', '.join(summary['topics']) or 'none'}")
        if missing:
            print(f"- FAIL missing topics: {', '.join(missing)}")
    print()


def _print_redis_status(summary: dict[str, Any]) -> None:
    print("Redis")
    if not summary["ok"]:
        print(f"- FAIL redis check: {summary['message']}")
    else:
        sample = ", ".join(summary["keys"][:5]) or "none"
        print(f"- OK   dbsize={summary['dbsize']} alert_keys={len(summary['keys'])}")
        print(f"- OK   sample_keys={sample}")
    print()


def _print_clickhouse_status(summary: dict[str, Any]) -> None:
    print("ClickHouse")
    if not summary["ok"]:
        print(f"- FAIL clickhouse check: {summary['message']}")
    else:
        print(f"- OK   row_count={summary['row_count']}")
    print()


def _print_health_summary(
    infra_status: list[dict[str, Any]],
    process_status: list[dict[str, Any]],
    redis_summary: dict[str, Any],
    clickhouse_summary: dict[str, Any],
) -> None:
    infra_ok = all(item["healthy"] for item in infra_status if item["required"])
    processes_ok = all(item["status"] == "running" for item in process_status)
    end_to_end_ok = (
        redis_summary.get("ok")
        and clickhouse_summary.get("ok")
        and (
            len(redis_summary.get("keys", [])) > 0
            or (clickhouse_summary.get("row_count") or 0) > 0
        )
    )

    print("Health Summary")
    print(
        f"- Infra running well: {'YES' if infra_ok else 'NO'}"
    )
    print(
        f"- Python services running well: {'YES' if processes_ok else 'NO'}"
    )
    print(
        f"- End-to-end data observed in Redis/ClickHouse: {'YES' if end_to_end_ok else 'NO'}"
    )
    if not end_to_end_ok:
        print(
            "- Note: the system may still be warming up. With the default 5-minute window and 10-minute watermark, Spark can need some time before producing final output."
        )
    print()


def _build_pythonpath(workdir: Path, existing_pythonpath: str | None) -> str:
    paths = [str(workdir)]
    if existing_pythonpath:
        paths.append(existing_pythonpath)
    return os.pathsep.join(paths)


def _load_state() -> dict[str, ProcessRuntime]:
    if not PID_FILE.exists():
        return {}

    payload = json.loads(PID_FILE.read_text(encoding="utf-8"))
    state: dict[str, ProcessRuntime] = {}
    for name, item in payload.items():
        state[name] = ProcessRuntime(**item)
    return state


def _save_state(state: dict[str, ProcessRuntime]) -> None:
    serializable = {name: asdict(runtime) for name, runtime in state.items()}
    PID_FILE.write_text(json.dumps(serializable, indent=2), encoding="utf-8")


def _is_pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _stop_runtime(runtime: ProcessRuntime) -> None:
    if not _is_pid_running(runtime.pid):
        print(f"Process already stopped: {runtime.name} pid={runtime.pid}")
        return

    print(f"Stopping {runtime.name} pid={runtime.pid}...")
    os.killpg(runtime.pgid, signal.SIGTERM)
    deadline = time.time() + 20

    while time.time() < deadline:
        if not _is_pid_running(runtime.pid):
            print(f"Stopped {runtime.name}.")
            return
        time.sleep(1)

    os.killpg(runtime.pgid, signal.SIGKILL)
    print(f"Force stopped {runtime.name}.")


def _tail_file(path: Path, lines: int) -> list[str]:
    content = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    return content[-lines:]


def _container_name(env: dict[str, str], suffix: str) -> str:
    return f"{env.get('COMPOSE_PROJECT_NAME', 'crypto-realtime-alert')}-{suffix}"


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
