import csv
import io
import json


def rows_to_csv(data):
    fieldnames = list(dict.fromkeys(key for row in data for key in row.keys()))

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore", restval="")
    writer.writeheader()
    writer.writerows(data)
    return output.getvalue()


def rows_to_json(data):
    return json.dumps(data, indent=2, ensure_ascii=False)
