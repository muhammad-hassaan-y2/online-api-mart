from fastapi import HTTPException
from sqlmodel import Session, select
from app.models.payment_model import Payment, PaymentUpdate

# Add a New Product to the Database
def create_payment(payment_data: Payment, session: Session):
    session.add(payment_data)
    session.commit()
    session.refresh(payment_data)
    return payment_data

def get_all_payments(session: Session):
    return session.exec(select(Payment)).all()

def get_payment_by_id(payment_id: int, session: Session):
    payment = session.exec(select(Payment).where(Payment.id == payment_id)).one_or_none()
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    return payment

def update_payment_by_id(payment_id: int, payment_data: PaymentUpdate, session: Session):
    payment = session.exec(select(Payment).where(Payment.id == payment_id)).one_or_none()
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    update_data = payment_data.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(payment, key, value)
    session.add(payment)
    session.commit()
    return payment

def delete_payment_by_id(payment_id: int, session: Session):
    payment = session.exec(select(Payment).where(Payment.id == payment_id)).one_or_none()
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    session.delete(payment)
    session.commit()
    return {"message": "Payment deleted successfully"}