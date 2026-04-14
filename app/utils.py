from datetime import datetime

def make_json_safe(data):
    if isinstance(data, dict):
        return {k: make_json_safe(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [make_json_safe(v) for v in data]
    elif isinstance(data, datetime):
        return data.isoformat()  # ✅ FIX
    else:
        return data