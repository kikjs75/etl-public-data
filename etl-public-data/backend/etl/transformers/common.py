from typing import Any

from etl.base import BaseTransformer


class MissingValueInterpolator(BaseTransformer):
    """Handles missing/null values via last-value carry-forward or defaults."""

    def __init__(self, numeric_fields: list[str], default: float = 0.0):
        self.numeric_fields = numeric_fields
        self.default = default

    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        last_values: dict[str, float] = {}
        for record in records:
            for field in self.numeric_fields:
                val = record.get(field)
                if val is None or val == "" or val == "-":
                    record[field] = last_values.get(field, self.default)
                else:
                    try:
                        record[field] = float(val)
                        last_values[field] = record[field]
                    except (ValueError, TypeError):
                        record[field] = last_values.get(field, self.default)
        return records


class UnitConverter(BaseTransformer):
    """Converts units (e.g., Fahrenheit to Celsius)."""

    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for record in records:
            if "temperature_f" in record:
                record["temperature"] = round((record["temperature_f"] - 32) * 5 / 9, 1)
                del record["temperature_f"]
        return records
