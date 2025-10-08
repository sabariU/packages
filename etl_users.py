from __future__ import annotations
from datetime import datetime
from pathlib import Path
from contextlib import closing

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook  # MariaDB-compatible

DEFAULT_ARGS = {"owner": "you", "retries": 1}
XCOM_DIR = Path("/opt/airflow/data/xcom")  # place to persist DataFrames between tasks
XCOM_DIR.mkdir(parents=True, exist_ok=True)

def _stamp() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%S")

@dag(
    dag_id="etl_users",
    description="Simple CSV -> Transform -> MariaDB 10.6 (DataFrame persisted between tasks)",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "mariadb", "demo"],
)
def etl_users():
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
        """Load DataFrame, clean/transform, persist again, and return new path."""
        df = pd.read_pickle(df_path)

        # Normalize/clean
        df["user_id"] = df["user_id"].astype(int)
        df["first_name"] = df["first_name"].str.strip().str.title()
        df["last_name"]  = df["last_name"].str.strip().str.title()
        df["email"]      = df["email"].str.strip().str.lower()

        # Parse ISO8601 -> naive UTC (MariaDB DATETIME friendly)
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True).dt.tz_convert(None)

        # Dedupe and order columns
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
        """Read transformed DataFrame and upsert into MariaDB."""
        df = pd.read_pickle(df_path)
        if df.empty:
            return "No rows to load."

        # Format created_at as 'YYYY-MM-DD HH:MM:SS' for DATETIME
        df = df.assign(created_at=df["created_at"].dt.strftime("%Y-%m-%d %H:%M:%S"))

        sql = (
            "INSERT INTO users (user_id, first_name, last_name, email, created_at, loaded_at) "
            "VALUES (%s, %s, %s, %s, %s, NOW()) "
            "ON DUPLICATE KEY UPDATE "
            "first_name=VALUES(first_name), "
            "last_name=VALUES(last_name), "
            "email=VALUES(email), "
            "created_at=VALUES(created_at), "
            "loaded_at=NOW()"
        )

        tuples = list(df.itertuples(index=False, name=None))  # [(user_id, first, last, email, created_at), ...]

        hook = MySqlHook(mysql_conn_id="mariadb_conn")
        with closing(hook.get_conn()) as conn, closing(conn.cursor()) as cur:
            cur.executemany(sql, tuples)
            conn.commit()

        return f"Loaded {len(tuples)} row(s) into MariaDB."
    
    @task()
    def verify_load() -> str:
        hook = MySqlHook(mysql_conn_id="mariadb_conn")
        # total count
        total = hook.get_first("SELECT COUNT(*) FROM users;")[0]
        # recent rows in the last hour
        recent = hook.get_first(
            "SELECT COUNT(*) FROM users WHERE loaded_at >= NOW() - INTERVAL 1 HOUR;"
        )[0]
        # sample rows
        sample_df = hook.get_pandas_df(
            "SELECT user_id, first_name, last_name, email, created_at, loaded_at "
            "FROM users ORDER BY loaded_at DESC LIMIT 5;"
        )
        # Log to task output
        print("TOTAL ROWS:", total)
        print("ROWS LOADED LAST HOUR:", recent)
        print(sample_df.to_string(index=False))
        return f"total={total}, recent={recent}"

    verify_load = verify_load()
    load_to_mariadb = load_to_mariadb(transform(extract_csv()))
    load_to_mariadb >> verify_load

etl_users()
