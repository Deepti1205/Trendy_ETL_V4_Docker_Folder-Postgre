import pandas as pd
import yaml
from scripts.logger import logger
from scripts.validators.schema_validator import validate_schema
from scripts.validators.business_validator import validate_business_rules


def load_schema(filename):
    schema_file = filename.replace(".csv", ".yaml")
    schema_path = f"validation/{schema_file}"

    logger.info(f"Loading validation schema: {schema_path}")

    with open(schema_path) as f:
        return yaml.safe_load(f)


def merge_quarantine_dfs(dfs):
    """
    Merge multiple quarantine dataframes while preserving error_reason.
    If a row appears multiple times → combine reasons.
    """
    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs)

    if "error_reason" not in combined.columns:
        return combined.reset_index(drop=True)

    # Combine reasons for duplicate rows
    combined = (
        combined.groupby(list(combined.columns.difference(["error_reason"])))
        ["error_reason"]
        .apply(lambda x: " | ".join(set(x)))
        .reset_index()
    )

    return combined


def validate(csv_path, filename):
    logger.info(f"Starting validation for file: {filename}")

    try:
        df = pd.read_csv(csv_path)
    except Exception:
        logger.exception(f"Failed to read CSV: {filename}")
        return False, ["CSV read error"], None, None

    schema = load_schema(filename)

    # --------------------------------------------------
    # SCHEMA VALIDATION
    # --------------------------------------------------
    schema_valid_df, schema_quarantine_df, schema_errors = validate_schema(
        df, schema
    )

    # File-level schema errors → reject file
    if schema_errors:
        logger.error(f"Schema validation failed for {filename}: {schema_errors}")
        return False, schema_errors, None, None

    # --------------------------------------------------
    # BUSINESS VALIDATION
    # --------------------------------------------------
    biz_valid_df, biz_quarantine_df = validate_business_rules(
        schema_valid_df, schema, filename
    )

    # --------------------------------------------------
    # MERGE QUARANTINE DATA
    # --------------------------------------------------
    quarantine_df = merge_quarantine_dfs([
        schema_quarantine_df,
        biz_quarantine_df
    ])

    valid_df = biz_valid_df

    invalid_count = len(quarantine_df)
    total_count = len(df)

    # --------------------------------------------------
    # FILE-LEVEL REJECTION
    # --------------------------------------------------
    file_rules = schema.get("file_level", {})
    max_invalid_pct = file_rules.get("reject_if_invalid_rows_percent_gt")

    if max_invalid_pct is not None and total_count > 0:
        invalid_pct = (invalid_count / total_count) * 100

        if invalid_pct > max_invalid_pct:
            logger.error(
                f"{filename} rejected | Invalid rows {invalid_pct:.2f}% "
                f"> allowed {max_invalid_pct}%"
            )
            return (
                False,
                [f"Invalid rows exceeded threshold ({invalid_pct:.2f}%)"],
                None,
                quarantine_df,
            )

    # --------------------------------------------------
    # FINAL RESULT
    # --------------------------------------------------
    if invalid_count > 0:
        logger.warning(
            f"{filename} validated with {invalid_count} invalid rows (quarantined)"
        )

    logger.info(f"Validation successful for file: {filename}")

    return True, None, valid_df, quarantine_df
