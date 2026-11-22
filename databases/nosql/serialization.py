# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _make_serializable(doc: dict) -> dict:
    result = {}
    for key, value in doc.items():
        if isinstance(value, dict):
            result[key] = _make_serializable(value)
        elif isinstance(value, list):
            result[key] = [
                _make_serializable(item) if isinstance(item, dict) else str(item)
                if not isinstance(item, (str, int, float, bool, type(None))) else item
                for item in value
            ]
        elif isinstance(value, (str, int, float, bool, type(None))):
            result[key] = value
        else:
            # Covers ObjectId, datetime, Decimal128, etc.
            result[key] = str(value)
    return result