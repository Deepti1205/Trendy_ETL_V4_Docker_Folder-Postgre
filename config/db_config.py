import os

DB = {
    "host": os.getenv("DB_HOST", "postgres-etl"),
    "user": os.getenv("DB_USER", "etl_user"),
    "password": os.getenv("DB_PASSWORD", "etl_pass"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "etl_db")
}
