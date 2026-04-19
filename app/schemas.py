# app/schemas.py

from pydantic import BaseModel

class LoginRequest(BaseModel):
    identifier: str  # username or email
    password: str

class AccountLotSchema(BaseModel):
    account_id: int
    lot_size: float