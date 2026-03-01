import psycopg2
import pandas as pd
from scripts.logger import logger
from scripts.load import DB


def fetch_reference_set(table, column):
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    cur.execute(f"SELECT {column} FROM {table}")
    values = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return values


def validate_referential_integrity(transactions_df: pd.DataFrame):
	"""
    Returns:
        valid_df
        quarantine_df (with error_reason column)
	"""

	df = transactions_df.copy()
	df.columns = df.columns.str.lower()
	
	customers = fetch_reference_set("customers", "customer_id")
	products = fetch_reference_set("products", "sku")
	
	invalid_customer = ~df["customerid"].isin(customers)
	invalid_sku = ~df["sku"].isin(products)
	
	# --------------------------------------------------
    # BUILD ROW-LEVEL ERROR REASONS
    # --------------------------------------------------
	row_errors = {}
	
	for idx in df.index:
		errors = []
		
		if invalid_customer.loc[idx]:
			errors.append("Invalid CustomerID (not found in customers table)")
			
		if invalid_sku.loc[idx]:
			errors.append("Invalid SKU (not found in products table)")
			
		if errors:
			row_errors[idx] = " | ".join(errors)
			
	 # --------------------------------------------------
    # SPLIT DATA
    # --------------------------------------------------
	quarantine_idx = list(row_errors.keys())
	
	quarantine_df = transactions_df.loc[quarantine_idx].copy()
	valid_df = transactions_df.drop(quarantine_idx).copy()
	
	if not quarantine_df.empty:
		quarantine_df["error_reason"] = quarantine_df.index.map(row_errors)
		
		logger.warning(
			f"Referential integrity failed | "
			f"Invalid CustomerID: {invalid_customer.sum()} | "
			f"Invalid SKU: {invalid_sku.sum()}"
		)
	
	return valid_df, quarantine_df
