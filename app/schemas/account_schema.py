# app/schemas/account_schema.py

from pydantic import BaseModel



class AccountLotSchema(BaseModel):
    account_id: int
    lot_size: float