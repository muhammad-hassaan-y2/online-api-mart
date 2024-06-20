from fastapi import HTTPException
from sqlmodel import Session, select
from app.models.notification_model import Notification, NotificationUpdate

def create_notification(notification_data: Notification, session: Session):
    session.add(notification_data)
    session.commit()
    session.refresh(notification_data)
    return notification_data

def get_all_notifications(session: Session):
    return session.exec(select(Notification)).all()

def get_notification_by_id(notification_id: int, session: Session):
    notification = session.exec(select(Notification).where(Notification.id == notification_id)).one_or_none()
    if not notification:
        raise HTTPException(status_code=404, detail="Notification not found")
    return notification

def update_notification_by_id(notification_id: int, notification_data: NotificationUpdate, session: Session):
    notification = session.exec(select(Notification).where(Notification.id == notification_id)).one_or_none()
    if not notification:
        raise HTTPException(status_code=404, detail="Notification not found")
    update_data = notification_data.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(notification, key, value)
    session.add(notification)
    session.commit()
    return notification

def delete_notification_by_id(notification_id: int, session: Session):
    notification = session.exec(select(Notification).where(Notification.id == notification_id)).one_or_none()
    if not notification:
        raise HTTPException(status_code=404, detail="Notification not found")
    session.delete(notification)
    session.commit()
    return {"message": "Notification deleted successfully"}
