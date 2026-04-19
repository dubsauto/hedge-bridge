from app.model import BotLog, SymbolMappingGroup, SymbolMappingEntry
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

def resolve_symbol(db, master_acc_id, slave_acc_id, symbol):
    group = db.query(SymbolMappingGroup)\
        .join(SymbolMappingEntry)\
        .filter(
            SymbolMappingEntry.account_id == master_acc_id,
            SymbolMappingEntry.symbol == symbol
        ).first()

    if not group:
        return symbol

    entry = db.query(SymbolMappingEntry).filter_by(
        group_id=group.id,
        account_id=slave_acc_id
    ).first()

    return entry.symbol if entry else symbol