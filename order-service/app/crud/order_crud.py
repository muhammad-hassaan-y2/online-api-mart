from fastapi import HTTPException
from sqlmodel import Session, select
from app.models.order_model import Order, OrderUpdate

def create_order(order_data: Order, session: Session):
    session.add(order_data)
    session.commit()
    session.refresh(order_data)
    return order_data

def get_all_orders(session: Session):
    return session.exec(select(Order)).all()

def get_order_by_id(order_id: int, session: Session):
    order = session.exec(select(Order).where(Order.id == order_id)).one_or_none()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order

def update_order_by_id(order_id: int, order_data: OrderUpdate, session: Session):
    order = session.exec(select(Order).where(Order.id == order_id)).one_or_none()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    update_data = order_data.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(order, key, value)
    session.add(order)
    session.commit()
    return order

def delete_order_by_id(order_id: int, session: Session):
    order = session.exec(select(Order).where(Order.id == order_id)).one_or_none()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    session.delete(order)
    session.commit()
    return {"message": "Order deleted successfully"}
