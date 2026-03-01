import yaml
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from scripts.logger import logger
from config.db_config import DB
import hashlib
from datetime import datetime

# --------------------------------------------------
# DB CONFIG
# --------------------------------------------------
"""DB = {
    "host": "localhost",
    "user": "postgres",
    "password": "deepti",
    "port": 5432,
    "database": "trendy_db"
}"""

# Explicit overrides for business-critical fields
COLUMN_TYPE_OVERRIDES = {
    "phone_no": "VARCHAR(15)",
    "pincode": "VARCHAR(6)",
    "email_id": "VARCHAR(255)"
}

# YAML → PostgreSQL type mapping
SCHEMA_TYPE_MAP = {
    "integer": "INTEGER",
    "number": "NUMERIC",
    "string": "TEXT",
    "timestamp": "TIMESTAMP"
}

# --------------------------------------------------
# SCD TYPE 2 CONFIG
# --------------------------------------------------
SCD2_CONFIG = {
    "customers": {
        "business_key": ["customer_id"],
        "tracked_columns": [
            "first_name",
			"last_name",
			"age",
			"marital_status",
			"children",
			"employment_status",
			"income_level",
			"address_line1",
			"address_line2",
            "phone_no",
            "email_id",
            "city",
            "state",
            "pincode"
        ]
    },
    "products": {
        "business_key": ["sku"],
        "tracked_columns": [
			"category_name",
			"sub-category",
			"product_name",
            "brand",
            "product_description",
            "gender",
			"color",
			"size",
            "selling_price",
            "discount"
        ]
    }
}

# --------------------------------------------------
# SCHEMA LOADER
# --------------------------------------------------
def load_schema(filename):
    schema_path = f"validation/{filename.replace('.csv', '.yaml')}"
    logger.info(f"Loading schema for DB load: {schema_path}")

    with open(schema_path,"r") as f:
        return yaml.safe_load(f)

# --------------------------------------------------
# DATABASE SETUP
# --------------------------------------------------
def create_database():
    try:
        conn = psycopg2.connect(
            host=DB["host"],
            user=DB["user"],
            password=DB["password"],
            port=DB["port"],
            database="postgres"
        )
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (DB["database"],)
        )

        if not cur.fetchone():
            logger.info(f"Creating database: {DB['database']}")
            cur.execute(f"CREATE DATABASE {DB['database']}")

        cur.close()
        conn.close()

    except Exception:
        logger.exception("Database creation/check failed")
        raise


def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{DB['user']}:{DB['password']}"
        f"@{DB['host']}:{DB['port']}/{DB['database']}"
    )

# --------------------------------------------------
# SQL TYPE INFERENCE
# --------------------------------------------------
def infer_sql_type(column: str, rules: dict) -> str:
	col = column.lower()
	
	if col in COLUMN_TYPE_OVERRIDES:
		return COLUMN_TYPE_OVERRIDES[col]
		
	yaml_type = rules.get("type", "string")
	
	if yaml_type == "integer":
		return "INTEGER"
		
	if "timestamp" in col:
		return "TIMESTAMP"
		
	if "min" in rules or "max" in rules:
		return "NUMERIC"
	
	# Optional length support
	if "max_length" in rules:
		return f"VARCHAR({rules['max_length']})"
		
	return "TEXT"
	
	
# --------------------------------------------------
# TABLE CREATION (SCHEMA-DRIVEN)
# --------------------------------------------------
def create_table(engine, table, schema):
	logger.info(f"Ensuring table exists: {table}")
	
	columns = schema.get("columns", {})
	ddl_cols = []
	constraints = []
	
	# --------------------------------------------------
    # SCD2 surrogate key
    # --------------------------------------------------
	is_scd2 = table in SCD2_CONFIG
	
	if is_scd2:
		ddl_cols.append(f"{table[:-1]}_sk BIGSERIAL PRIMARY KEY")
		
	# --------------------------------------------------
    # Regular columns from YAML
    # --------------------------------------------------	
	for col, rules in columns.items():
		col_l = col.lower()
		sql_type = infer_sql_type(col_l,rules)
		
		col_def = f'"{col_l}" {sql_type}'
		
		if rules.get("required", False):
			col_def += " NOT NULL"
		
		if rules.get("unique", False):
			if not (is_scd2 and col_l in SCD2_CONFIG[table]["business_key"]):
				constraints.append(f"UNIQUE ({col_l})")
			
		ddl_cols.append(col_def)
		
	# --------------------------------------------------
    # SCD2 system columns
    # --------------------------------------------------
	if is_scd2:
		ddl_cols.extend([
            "record_hash TEXT NOT NULL",
            "effective_from TIMESTAMP NOT NULL",
            "effective_to TIMESTAMP",
            "is_current BOOLEAN NOT NULL"
        ])
		
		# Business key uniqueness (only one current row)
		bk = SCD2_CONFIG[table]["business_key"]
	
		constraints.append(
            f"UNIQUE ({', '.join(bk)}, is_current)"
			)
		
	ddl_sql = f"""
				CREATE TABLE IF NOT EXISTS {table}(
				{', '.join(ddl_cols)}
				{',' if constraints else ''}
				{', '.join(constraints)}
			);
		"""
	
	try:
		with engine.begin() as conn:
			conn.execute(text(ddl_sql))
	except Exception:
		logger.exception(f"Table creation failed: {table}")
		raise

# --------------------------------------------------
# Generate Hash
# --------------------------------------------------
def generate_record_hash(row: pd.Series, tracked_cols: list) -> str:
	values = [
		str(row[col]) if col in row and pd.notna(row[col]) else ""
		for col in tracked_cols
		]
	return hashlib.md5("|".join(values).encode()).hexdigest()
	
# --------------------------------------------------
# SCD2 Upsert
# --------------------------------------------------
def scd2_upsert(engine, table: str, df: pd.DataFrame):
	config = SCD2_CONFIG[table]
	business_keys = config["business_key"]
	tracked_cols = config["tracked_columns"]

	logger.info(f"SCD2 upsert started for table: {table}")

	df = df.copy()

	df["record_hash"] = df.apply(
		lambda r: generate_record_hash(r, tracked_cols), axis=1
	)
    
	df["effective_from"] = datetime.now()
	df["effective_to"] = None
	df["is_current"] = True
	
	insert_count = update_count = skip_count = 0

	with engine.begin() as conn:
		for _, row in df.iterrows():

			where_clause = " AND ".join(
				[f"{k} = :{k}" for k in business_keys]
			)

			select_sql = f"""
				SELECT record_hash
				FROM {table}
				WHERE {where_clause}
				AND is_current = TRUE
			"""

			params = {k: row[k] for k in business_keys}
			existing = conn.execute(
				text(select_sql), params
			).fetchone()

            # Case 1: new business key
			if not existing:
				row.to_frame().T.to_sql(
					table, conn, index=False, if_exists="append"
				)
				insert_count += 1
				#logger.info(f"SCD2 INSERT → New record created in {table}")

            # Case 2: changed record
			elif existing[0] != row["record_hash"]:
				
				expire_sql = f"""
						UPDATE {table} SET effective_to = :effective_to, is_current = FALSE
						WHERE {where_clause} AND is_current = TRUE
						"""
				conn.execute(
					text(expire_sql),
					{
                        **params,
                        "effective_to": datetime.now()
                    }
					
				)
				
				row.to_frame().T.to_sql(table, conn, index=False, if_exists="append")
				update_count += 1
				
				#logger.info(f"SCD2 UPDATE → Existing record expired and new version inserted in {table}")
				
			# Case 3: no change → do nothing
			else:
				skip_count += 1
				#logger.info(f"SCD2 SKIP: No changes detected in {table}") #{params} will give each row in customers
				continue
		
		logger.info(f"SCD2 SUMMARY : {table} | INSERT: {insert_count} | UPDATE: {update_count} | SKIP: {skip_count}")
		
# --------------------------------------------------
# METADATA TABLE
# --------------------------------------------------
def create_metadata_table(engine):
	sql = """
		CREATE TABLE IF NOT EXISTS etl_file_metadata (
			id SERIAL PRIMARY KEY,
			file_name TEXT,
			file_hash TEXT UNIQUE,
			processed_at TIMESTAMP,
			status TEXT
		);
	"""
	
	with engine.begin() as conn:
		conn.execute(text(sql))
		
# --------------------------------------------------
# FILE HASH
# --------------------------------------------------
def generate_file_hash(df: pd.DataFrame) -> str:
	csv_bytes = df.to_csv(index=False).encode()
	return hashlib.md5(csv_bytes).hexdigest()

# --------------------------------------------------
# Check If File Already Processed
# --------------------------------------------------
def is_file_processed(engine, file_hash):
	sql = """
		SELECT 1 FROM etl_file_metadata
		WHERE file_hash = :hash AND status = 'SUCCESS'
	"""
	with engine.begin() as conn:
		return conn.execute(text(sql), {"hash": file_hash}).fetchone() is not None

# --------------------------------------------------
# Insert Metadata Record
# --------------------------------------------------
def record_file_metadata(engine, filename, file_hash, status):
	sql = """
		INSERT INTO etl_file_metadata (file_name, file_hash, processed_at, status)
		VALUES (:name, :hash, :time, :status)
	"""
	
	with engine.begin() as conn:
		conn.execute(text(sql), {
			"name": filename,
			"hash": file_hash,
			"time": datetime.now(),
			"status": status
		})
		
# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
def load(df: pd.DataFrame, filename: str):
	table = filename.replace(".csv", "").lower()
	logger.info(f"Starting load → {filename} → table: {table}")

	try:
		schema = load_schema(filename)

		create_database()
		engine = get_engine()
		
		create_metadata_table(engine)

        # Normalize column names
		df.columns = [c.lower() for c in df.columns]
		
		# --------------------------------------------------
        # FILE HASH CHECK
        # --------------------------------------------------
		
		file_hash = generate_file_hash(df)
		
		if is_file_processed(engine, file_hash):
			logger.warning(f"File already processed — skipping: {filename}")
			return
		
		# --------------------------------------------------
        # LOAD DATA
        # --------------------------------------------------
		
		create_table(engine, table, schema)
		
		if table in SCD2_CONFIG:
			scd2_upsert(engine,table,df)
		else:
			logger.info(f"Inserting {len(df)} rows into {table}")
			df.to_sql(
            table,
            engine,
            if_exists="append",
            index=False,
            method="multi"
			)
		
		# --------------------------------------------------
        # RECORD SUCCESS
        # --------------------------------------------------
		record_file_metadata(engine, filename, file_hash, "SUCCESS")
		
		logger.info(f"Load successful for file: {filename}")

	except Exception:
		record_file_metadata(engine, filename, file_hash, "FAILED")
		logger.exception(f"LOAD FAILED for file: {filename}")
		raise
