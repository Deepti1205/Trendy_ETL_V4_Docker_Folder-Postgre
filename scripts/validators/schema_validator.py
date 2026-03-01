import pandas as pd
from scripts.logger import logger

TYPE_CASTERS = {
	"int": lambda s: pd.to_numeric(s, errors="coerce").astype("Int64"),
	"float": lambda s: pd.to_numeric(s, errors="coerce"),
	"string": lambda s: s.astype(str),
	"timestamp": lambda s: pd.to_datetime(s, errors="coerce"),
}


def validate_schema(df: pd.DataFrame, schema: dict):
	
	"""
			Returns:
					valid_df        → rows passing schema rules
					quarantine_df   → rows violating schema rules
					errors          → file-level errors only
	"""
	errors = []
	columns = schema.get("columns", {})
	
	# NEW: Track row-level errors 
	row_errors = {idx: [] for idx in df.index}


# --------------------------------------------------
# FILE-LEVEL: Missing columns → reject file
# --------------------------------------------------
	missing = [col for col in columns if col not in df.columns]
	if missing:
		errors.extend([f"Missing column: {col}" for col in missing])
		logger.error(f"Schema validation failed | Missing columns: {missing}")
		return None, None, errors
	
	 # --------------------------------------------------
#  TYPE CASTING (schema-driven)
# --------------------------------------------------
	for col, rules in columns.items():
		col_type = rules.get("type", "string")

		if col_type not in TYPE_CASTERS:
			errors.append(f"Unsupported type '{col_type}' for column '{col}'")
			continue

		original = df[col]
		casted = TYPE_CASTERS[col_type](original)
			
		# Identify failed casts
		failed_mask = original.notna() & casted.isna()
			
		# rows that failed type casting
		for idx in df[failed_mask].index:
			row_errors[idx].append(f"{col}: invalid {col_type}")
			
		df[col] = casted
	
	
	# --------------------------------------------------
	# ROW-LEVEL: Column rules → quarantine rows
	# --------------------------------------------------
	for col, rules in columns.items():
		series = df[col]

		# Required
		if rules.get("required", False):
			for idx in series[series.isnull()].index:
				row_errors[idx].append(f"{col}: required")
		

		# Allowed values
		if "allowed_values" in rules:
			mask = series.notna() & ~series.isin(rules["allowed_values"])
			for idx in series[mask].index:
				row_errors[idx].append(f"{col}: invalid value")
							
		# Regex
		regex = rules.get("regex") or rules.get("reg_ex")
		if regex:
			mask = series.notna() & ~series.astype(str).str.match(regex)
			for idx in series[mask].index:
				row_errors[idx].append(f"{col}: regex failed")
			   

		# Min / Max (numeric checks)
		numeric = pd.to_numeric(series, errors="coerce")

		if "min" in rules:
			for idx in numeric[numeric < rules["min"]].index:
				row_errors[idx].append(f"{col}: below min {rules['min']}")
		

		if "max" in rules:
			for idx in numeric[numeric > rules["max"]].index:
				row_errors[idx].append(f"{col}: above max {rules['max']}")
		

	# --------------------------------------------------
	# SPLIT DATA
	# --------------------------------------------------
	error_series = pd.Series(
					{idx: " | ".join(msgs) for idx, msgs in row_errors.items() if msgs},dtype='object'
					)
	
	
	quarantine_df = df.loc[error_series.index].copy()
	quarantine_df["error_reason"] = error_series
	
	valid_df = df.drop(error_series.index).copy()

	logger.info(
		f"Schema validation completed | "
		f"Valid rows: {len(valid_df)} | "
		f"Quarantined rows: {len(quarantine_df)}"
	)

	return valid_df, quarantine_df, errors
