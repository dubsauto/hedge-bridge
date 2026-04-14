# app/schemas.py

from pydantic import BaseModel

class LoginRequest(BaseModel):
    identifier: str  # username or email
    password: str