import pandas as pd
from scripts.logger import logger
from datetime import timedelta


def evaluate_condition(row, condition):
    try:
        context = {
            "now": pd.Timestamp.now(),
            "timedelta": timedelta
        }
        return bool(eval(condition, context, row.to_dict()))
    except Exception as e:
        logger.error(f"Business rule eval failed | {condition} | {e}")
        return False


def validate_business_rules(df: pd.DataFrame, schema: dict, filename: str):
	"""
		Returns:
		valid_df
        quarantine_df (with error_reason column)
	"""
	
	rules = schema.get("business_rules", [])
	
	# NEW → Track row-level rule failures
	row_errors = {idx: [] for idx in df.index}

	valid_df = df.copy()
	quarantine_df = pd.DataFrame(columns=df.columns)

	for rule in rules:
		condition = rule.get("condition")
		action = rule.get("action")
		name = rule.get("name", "unnamed_rule")

		if not condition or action != "reject_row":
			continue
		
		# Evaluate rule row-wise
		mask = df.apply(
			lambda row: evaluate_condition(row, condition),
			axis=1
		)

		if mask.any():
			logger.warning(
				f"{filename} | Business rule failed: {name} | Rows: {mask.sum()}"
			)
			
			# Store error message per failed row
			for idx in df[mask].index:
				row_errors[idx].append(f"Business Rule Failed: {name}")
            
    # --------------------------------------------------
    # BUILD ERROR COLUMN
    # --------------------------------------------------
	error_series = pd.Series(
		{idx: " | ".join(msgs) for idx, msgs in row_errors.items() if msgs},dtype='object'
		)
		
	quarantine_df = df.loc[error_series.index].copy()
	quarantine_df["error_reason"] = error_series
	
	valid_df = df.drop(error_series.index).copy() 
            

	return valid_df, quarantine_df
