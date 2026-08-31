import csv
import io
import json


CSV_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


def _safe_csv_value(value):
    if isinstance(value, str) and value.startswith(CSV_FORMULA_PREFIXES):
        return f"'{value}"
    return value


def rows_to_csv(data):
    fieldnames = list(dict.fromkeys(key for row in data for key in row.keys()))

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore", restval="")
    writer.writeheader()
    writer.writerows(
        {key: _safe_csv_value(value) for key, value in row.items()}
        for row in data
    )
    return output.getvalue()


def rows_to_json(data):
    return json.dumps(data, indent=2, ensure_ascii=False)
