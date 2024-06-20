from typing import List, Optional
from sqlmodel import SQLModel, Field, Relationship

class Order(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    customer_id: int
    order_date: str
    status: str
    total_amount: float
    order_items: List["OrderItem"] = Relationship(back_populates="order")

class OrderItem(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: int = Field(foreign_key="order.id")
    product_id: int
    quantity: int
    price: float
    order: "Order" = Relationship(back_populates="order_items")

class OrderUpdate(SQLModel):
    customer_id: Optional[int] = None
    order_date: Optional[str] = None
    status: Optional[str] = None
    total_amount: Optional[float] = None
