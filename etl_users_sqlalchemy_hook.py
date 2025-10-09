from __future__ import annotations
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook  # MariaDB-compatible

DEFAULT_ARGS = {"owner": "you", "retries": 1}
XCOM_DIR = Path("/opt/airflow/data/xcom")
XCOM_DIR.mkdir(parents=True, exist_ok=True)

def _stamp() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%S")

@dag(
    dag_id="etl_users_sqlalchemy_hook",
    description="CSV -> Transform -> MariaDB via SQLAlchemy (engine from MySqlHook)",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "mariadb", "sqlalchemy", "mysqlhook"],
)
def etl_users_sqlalchemy():

    @task()
    def extract_csv() -> str:
        """Read CSV into a DataFrame and persist to disk. Return the file path."""
        csv_path = Path("/opt/airflow/data/users.csv")
        df = pd.read_csv(csv_path, dtype=str).fillna("")
        fp = XCOM_DIR / f"users_extract_{_stamp()}.pkl"
        df.to_pickle(fp)
        return str(fp)

    @task()
    def transform(df_path: str) -> str:
        """Load DataFrame, clean/dedupe, persist again, and return new path."""
        df = pd.read_pickle(df_path)

        df["user_id"] = df["user_id"].astype(int)
        df["first_name"] = df["first_name"].str.strip().str.title()
        df["last_name"]  = df["last_name"].str.strip().str.title()
        df["email"]      = df["email"].str.strip().str.lower()
        # ISO8601 -> naive UTC for DATETIME
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True).dt.tz_convert(None)

        df = (
            df.sort_values("created_at")
              .drop_duplicates(subset=["user_id", "email"], keep="last")
              .loc[:, ["user_id", "first_name", "last_name", "email", "created_at"]]
        )

        out_fp = XCOM_DIR / f"users_transform_{_stamp()}.pkl"
        df.to_pickle(out_fp)
        return str(out_fp)

    @task()
    def load_to_mariadb(df_path: str) -> str:
        """Upsert into MariaDB using SQLAlchemy engine obtained from MySqlHook."""
        df = pd.read_pickle(df_path)
        if df.empty:
            return "No rows to load."

        # Format created_at for MariaDB DATETIME
        df = df.assign(created_at=df["created_at"].dt.strftime("%Y-%m-%d %H:%M:%S"))

        # Get engine from Airflow connection via MySqlHook
        hook = MySqlHook(mysql_conn_id="mariadb_conn")
        engine = hook.get_sqlalchemy_engine(engine_kwargs={"pool_pre_ping": True})

        stmt = text(
            """
            INSERT INTO users (user_id, first_name, last_name, email, created_at, loaded_at)
            VALUES (:user_id, :first_name, :last_name, :email, :created_at, NOW())
            ON DUPLICATE KEY UPDATE
              first_name=VALUES(first_name),
              last_name=VALUES(last_name),
              email=VALUES(email),
              created_at=VALUES(created_at),
              loaded_at=NOW()
            """
        )

        payload = df.to_dict(orient="records")  # list[dict] for executemany
        with engine.begin() as conn:
            conn.execute(stmt, payload)

        return f"Loaded {len(payload)} row(s) into MariaDB via SQLAlchemy + MySqlHook."

    load_to_mariadb(transform(extract_csv()))

etl_users_sqlalchemy()
