from sqlalchemy import create_engine, text
from scripts.logger import logger
from config.db_config import DB


# --------------------------------------------------
# DB ENGINE
# --------------------------------------------------
def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{DB['user']}:{DB['password']}"
        f"@{DB['host']}:{DB['port']}/{DB['database']}"
    )


# --------------------------------------------------
# ADD SURROGATE KEY
# --------------------------------------------------
def add_surrogate_key(engine, table, sk_column):
    logger.info(f"Ensuring surrogate key on {table}")

    sql = f"""
    ALTER TABLE {table}
    ADD COLUMN IF NOT EXISTS {sk_column} SERIAL PRIMARY KEY;
    """

    with engine.begin() as conn:
        conn.execute(text(sql))


# --------------------------------------------------
# ADD SCD2 COLUMNS
# --------------------------------------------------
def add_scd2_columns(engine, table):
    logger.info(f"Ensuring SCD2 columns on {table}")

    sql = f"""
    ALTER TABLE {table}
        ADD COLUMN IF NOT EXISTS effective_from TIMESTAMP,
        ADD COLUMN IF NOT EXISTS effective_to TIMESTAMP,
        ADD COLUMN IF NOT EXISTS is_current BOOLEAN,
        ADD COLUMN IF NOT EXISTS record_hash TEXT;
    """

    with engine.begin() as conn:
        conn.execute(text(sql))


# --------------------------------------------------
# BACKFILL EXISTING ROWS
# --------------------------------------------------
def backfill_existing_rows(engine, table):
    logger.info(f"Backfilling SCD2 columns for {table}")

    sql = f"""
    UPDATE {table}
    SET effective_from = COALESCE(effective_from, NOW()),
        effective_to   = NULL,
        is_current     = COALESCE(is_current, TRUE),
        record_hash    = COALESCE(record_hash, 'INIT')
    WHERE effective_from IS NULL
       OR is_current IS NULL;
    """

    with engine.begin() as conn:
        conn.execute(text(sql))


# --------------------------------------------------
# MAIN MIGRATION
# --------------------------------------------------
def run():
    engine = get_engine()

    migrations = [
        ("customers", "customer_sk"),
        ("products", "product_sk"),
    ]

    for table, sk in migrations:
        add_surrogate_key(engine, table, sk)
        add_scd2_columns(engine, table)
        backfill_existing_rows(engine, table)

    logger.info("✅ V3 SCD2 schema migration completed successfully")


if __name__ == "__main__":
    run()
