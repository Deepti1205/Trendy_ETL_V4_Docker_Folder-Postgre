import json
import os


def generate_dq_report(filename, total_rows, valid_rows, invalid_rows):
    dq_score = round((valid_rows / total_rows) * 100, 2) if total_rows else 0

    report = {
        "file": filename,
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "dq_score": dq_score
    }

    os.makedirs("dq_reports", exist_ok=True)

    report_file = filename.replace(".csv", "_dq.json")

    with open(os.path.join("dq_reports", report_file), "w") as f:
        json.dump(report, f, indent=4)

    return dq_score, report
