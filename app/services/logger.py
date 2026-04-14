from app.model import BotLog
from app.utils import make_json_safe



def log(db, account_id, level, category, message, raw_json=None):
    try:
        entry = BotLog(
            account_id=account_id,
            level=level,
            category=category,
            message=message,
            raw_json=make_json_safe(raw_json) if raw_json else None
        )
        db.add(entry)
        db.commit()

    except Exception as e:
        db.rollback()  # ✅ VERY IMPORTANT
        print(f"❌ Logging failed: {e}")