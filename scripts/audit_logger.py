import psycopg2
from scripts.load import DB
from scripts.logger import logger


def log_audit(
    filename,
    table_name,
    status,
    total_rows=0,
    valid_rows=0,
    invalid_rows=0,
    dq_score=None,
    error_message=None
):
    conn = None
    try:
        conn = psycopg2.connect(**DB)
        cur = conn.cursor()

        # Ensure audit table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS etl_audit (
                id SERIAL PRIMARY KEY,
                filename TEXT,
                table_name TEXT,
                status TEXT,
                total_rows INT,
                valid_rows INT,
                invalid_rows INT,
                dq_score NUMERIC,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            INSERT INTO etl_audit (
                filename,
                table_name,
                status,
                total_rows,
                valid_rows,
                invalid_rows,
                dq_score,
                error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            filename,
            table_name,
            status,
            total_rows,
            valid_rows,
            invalid_rows,
            dq_score,
            error_message
        ))

        conn.commit()
        cur.close()

    except Exception:
        logger.exception("Failed to write audit log")

    finally:
        if conn:
            conn.close()
