from typing import Optional
from sqlmodel import SQLModel, Field

class Notification(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    type: str
    message: str
    status: str
    created_at: str

class NotificationUpdate(SQLModel):
    type: Optional[str] = None
    message: Optional[str] = None
    status: Optional[str] = None
