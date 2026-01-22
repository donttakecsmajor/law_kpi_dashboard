import time
import streamlit as st
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import OperationalError


def get_conn():
    # Needs .streamlit/secrets.toml:
    # [connections.neon]
    # url="postgresql://..."
    return st.connection("neon", type="sql")


def _run_with_retry(fn, retries: int = 3, base_sleep: float = 0.6):
    """
    Retries transient DB/network errors (e.g., 'SSL connection has been closed unexpectedly')
    """
    last_err = None
    for i in range(retries):
        try:
            return fn()
        except OperationalError as e:
            last_err = e
            time.sleep(base_sleep * (2 ** i))
    raise last_err


def init_db():
    """
    Create tables if they don't exist.
    Run DDL statements separately (more robust than sending one giant multi-statement batch).
    """

    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS settlements (
            id BIGSERIAL PRIMARY KEY,
            person_name TEXT NOT NULL,
            client_name TEXT NOT NULL,
            settlement_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
            policy_limits DOUBLE PRECISION NOT NULL DEFAULT 0,
            fee_earned DOUBLE PRECISION NOT NULL DEFAULT 0,
            settlement_date DATE NOT NULL,
            tod TEXT,
            track TEXT NOT NULL DEFAULT 'unknown',
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS pre_suit_kpis (
            id BIGSERIAL PRIMARY KEY,
            person_name TEXT NOT NULL,
            month TEXT NOT NULL, -- YYYY-MM
            demands_sent INTEGER NOT NULL DEFAULT 0,
            settlements_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
            avg_lien_resolution_days DOUBLE PRECISION NOT NULL DEFAULT 0,
            files_without_14_day_contact INTEGER NOT NULL DEFAULT 0,
            nps_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            active_case_load INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE(person_name, month)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
    ]

    def do():
        conn = get_conn()
        with conn.session as s:
            for stmt in ddl_statements:
                s.execute(text(stmt))
            s.commit()

        seed = """
        INSERT INTO settings(key, value)
        VALUES
          ('revenue_goal_2026', '0'),
          ('google_reviews_baseline', '221'),
          ('google_reviews_current', '221')
        ON CONFLICT (key) DO NOTHING;
        """
        with conn.session as s:
            s.execute(text(seed))
            s.commit()

    _run_with_retry(do)


def execute(query: str, params: dict | None = None):
    def do():
        conn = get_conn()
        with conn.session as s:
            s.execute(text(query), params or {})
            s.commit()

    _run_with_retry(do)


def query_df(query: str, params: dict | None = None) -> pd.DataFrame:
    def do():
        conn = get_conn()
        return conn.query(query, params=params or {}, ttl=0)

    return _run_with_retry(do)


def get_setting(key: str, default="0") -> str:
    df = query_df("SELECT value FROM settings WHERE key = :key", {"key": key})
    if df.empty:
        return default
    return str(df.loc[0, "value"])


def set_setting(key: str, value: str):
    execute(
        """
        INSERT INTO settings(key, value, updated_at)
        VALUES (:key, :value, now())
        ON CONFLICT (key) DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = now()
        """,
        {"key": key, "value": value},
    )
