"""Run the small fixture through the local end-to-end pipeline.

This is the integration path used by CI. It intentionally calls the same CLI
scripts used by Airflow instead of reimplementing pipeline logic here.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "olist_analytics"
DEFAULT_ARCHIVE = (
    PROJECT_ROOT / "tests" / "fixtures" / "olist_small" / "olist_small.zip"
)
DEFAULT_PROFILE = (
    PROJECT_ROOT / "tests" / "fixtures" / "olist_small" / "source_profile_small.json"
)
DEFAULT_RAW_DIR = PROJECT_ROOT / "data" / "ci" / "raw" / "olist_small"
POSTGRES_SQL_DIR = PROJECT_ROOT / "infra" / "postgres"
RESET_SCHEMAS = (
    "raw",
    "audit",
    "staging",
    "intermediate",
    "snapshots",
    "core",
    "marts",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", default=str(DEFAULT_ARCHIVE))
    parser.add_argument("--profile", default=str(DEFAULT_PROFILE))
    parser.add_argument("--raw-dir", default=str(DEFAULT_RAW_DIR))
    parser.add_argument("--batch-date", default="2018-09-01")
    parser.add_argument("--batch-id", default="2018-09-01")
    parser.add_argument("--run-id", default="ci_fixture")
    parser.add_argument("--dag-id", default="github_actions_fixture")
    parser.add_argument("--lookback-days", type=int, default=3)
    parser.add_argument("--dead-letter-max-rows", type=int, default=0)
    parser.add_argument("--dead-letter-max-rate", type=float, default=0)
    parser.add_argument("--dbt-threads", type=int, default=1)
    parser.add_argument("--dbt-bin")
    parser.add_argument("--skip-dbt", action="store_true")
    parser.add_argument("--reset-warehouse", action="store_true")
    return parser.parse_args()


def pipeline_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("POSTGRES_HOST", "localhost")
    env.setdefault("POSTGRES_PORT", "5432")
    env.setdefault("POSTGRES_DB", "olist_analytics")
    env.setdefault("POSTGRES_USER", "olist")
    env.setdefault("POSTGRES_PASSWORD", "olist")
    env.setdefault("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR))
    env.setdefault("DBT_TARGET", "local_pg")
    return env


def run(
    command: list[str], cwd: Path = PROJECT_ROOT, env: dict[str, str] | None = None
) -> None:
    print(f"+ {' '.join(command)}", flush=True)
    subprocess.run(command, cwd=cwd, env=env, check=True)


def dbt_command(args: argparse.Namespace) -> str:
    if args.dbt_bin:
        return args.dbt_bin

    executable = shutil.which("dbt")
    if executable:
        return executable

    candidate = Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt")
    if candidate.exists():
        return str(candidate)

    raise FileNotFoundError("Could not find dbt. Install project requirements first.")


def reset_warehouse(env: dict[str, str]) -> None:
    connection = psycopg2.connect(
        host=env["POSTGRES_HOST"],
        port=int(env["POSTGRES_PORT"]),
        dbname=env["POSTGRES_DB"],
        user=env["POSTGRES_USER"],
        password=env["POSTGRES_PASSWORD"],
    )
    try:
        with connection.cursor() as cursor:
            for schema in RESET_SCHEMAS:
                cursor.execute(f"drop schema if exists {schema} cascade;")
        connection.commit()
    finally:
        connection.close()


def mark_batch(
    status: str,
    args: argparse.Namespace,
    env: dict[str, str],
    error_message: str | None = None,
) -> None:
    command = [
        sys.executable,
        "scripts/orchestration/batch_control.py",
        "mark" if status != "FAILED" else "fail",
        "--batch-date",
        args.batch_date,
        "--batch-id",
        args.batch_id,
        "--run-id",
        args.run_id,
        "--dag-id",
        args.dag_id,
        "--raw-dir",
        args.raw_dir,
    ]
    if status != "FAILED":
        command.extend(["--status", status])
    if error_message:
        command.extend(["--error-message", error_message[:65535]])
    run(command, env=env)


def run_pipeline(args: argparse.Namespace, env: dict[str, str]) -> None:
    dbt = dbt_command(args)
    dbt_vars = json.dumps(
        {
            "batch_date": args.batch_date,
            "lookback_days": args.lookback_days,
        }
    )
    snapshot_vars = json.dumps({"batch_date": args.batch_date})

    if args.reset_warehouse:
        reset_warehouse(env)

    run(
        [
            sys.executable,
            "scripts/orchestration/batch_control.py",
            "start",
            "--bootstrap-sql-dir",
            str(POSTGRES_SQL_DIR),
            "--batch-date",
            args.batch_date,
            "--batch-id",
            args.batch_id,
            "--run-id",
            args.run_id,
            "--dag-id",
            args.dag_id,
            "--raw-dir",
            args.raw_dir,
        ],
        env=env,
    )
    run(
        [
            sys.executable,
            "scripts/utilities/validate_source_contract.py",
            "--archive",
            args.archive,
            "--profile",
            args.profile,
        ],
        env=env,
    )
    mark_batch("SOURCE_VALIDATED", args, env)
    run(
        [
            sys.executable,
            "scripts/ingestion/prepare_olist_raw_files.py",
            "--archive",
            args.archive,
            "--profile",
            args.profile,
            "--output-dir",
            args.raw_dir,
            "--batch-date",
            args.batch_date,
            "--batch-id",
            args.batch_id,
            "--run-id",
            args.run_id,
            "--dead-letter-max-rows",
            str(args.dead_letter_max_rows),
            "--dead-letter-max-rate",
            str(args.dead_letter_max_rate),
        ],
        env=env,
    )
    run(
        [
            sys.executable,
            "scripts/ingestion/generate_correction_feeds.py",
            "--archive",
            args.archive,
            "--output-dir",
            args.raw_dir,
            "--batch-date",
            args.batch_date,
            "--batch-id",
            args.batch_id,
            "--run-id",
            args.run_id,
            "--dead-letter-max-rows",
            str(args.dead_letter_max_rows),
            "--dead-letter-max-rate",
            str(args.dead_letter_max_rate),
        ],
        env=env,
    )
    mark_batch("RAW_PREPARED", args, env)
    run(
        [
            sys.executable,
            "scripts/loading/load_raw_to_postgres.py",
            "--raw-dir",
            args.raw_dir,
            "--profile",
            args.profile,
            "--bootstrap-sql-dir",
            str(POSTGRES_SQL_DIR),
            "--batch-date",
            args.batch_date,
            "--batch-id",
            args.batch_id,
            "--run-id",
            args.run_id,
            "--dag-id",
            args.dag_id,
        ],
        env=env,
    )
    run(
        [
            sys.executable,
            "scripts/quality/reconcile_batch.py",
            "--raw-dir",
            args.raw_dir,
            "--profile",
            args.profile,
            "--bootstrap-sql-dir",
            str(POSTGRES_SQL_DIR),
            "--batch-date",
            args.batch_date,
            "--batch-id",
            args.batch_id,
            "--run-id",
            args.run_id,
            "--dag-id",
            args.dag_id,
        ],
        env=env,
    )

    if args.skip_dbt:
        return

    run(
        [
            dbt,
            "run",
            "--select",
            "staging",
            "intermediate",
            "--threads",
            str(args.dbt_threads),
            "--vars",
            snapshot_vars,
        ],
        cwd=DBT_PROJECT_DIR,
        env=env,
    )
    mark_batch("DBT_SNAPSHOT_INPUTS_BUILT", args, env)
    run(
        [
            dbt,
            "snapshot",
            "--threads",
            str(args.dbt_threads),
            "--vars",
            snapshot_vars,
        ],
        cwd=DBT_PROJECT_DIR,
        env=env,
    )
    mark_batch("DBT_SNAPSHOTTED", args, env)
    run(
        [
            dbt,
            "build",
            "--exclude",
            "resource_type:snapshot",
            "--full-refresh",
            "--threads",
            str(args.dbt_threads),
            "--vars",
            dbt_vars,
        ],
        cwd=DBT_PROJECT_DIR,
        env=env,
    )
    mark_batch("DBT_BUILT", args, env)
    run(
        [dbt, "test", "--threads", str(args.dbt_threads), "--vars", dbt_vars],
        cwd=DBT_PROJECT_DIR,
        env=env,
    )
    mark_batch("TESTED", args, env)


def main() -> None:
    args = parse_args()
    env = pipeline_env()
    try:
        run_pipeline(args, env)
    except subprocess.CalledProcessError as exc:
        try:
            mark_batch("FAILED", args, env, error_message=str(exc))
        except Exception as mark_exc:
            print(f"Could not mark batch failed: {mark_exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
