from typing import Optional
from sqlmodel import SQLModel, Field

class Payment(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: int
    amount: float
    status: str
    transaction_date: str

class PaymentUpdate(SQLModel):
    amount: Optional[float] = None
    status: Optional[str] = None