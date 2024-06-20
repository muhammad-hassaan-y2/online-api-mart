from fastapi import HTTPException
from sqlmodel import Session, select
from app.models.user_model import User, UserUpdate
from passlib.context import CryptContext


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_password_hash(password):
    return pwd_context.hash(password)

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def create_user(user_data: User, session: Session):
    user_data.hashed_password = get_password_hash(user_data.hashed_password)
    session.add(user_data)
    session.commit()
    session.refresh(user_data)
    return user_data

def get_user_by_username(username: str, session: Session):
    user = session.exec(select(User).where(User.username == username)).one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

def get_all_users(session: Session):
    return session.exec(select(User)).all()

def update_user_by_id(user_id: int, user_data: UserUpdate, session: Session):
    user = session.exec(select(User).where(User.id == user_id)).one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    update_data = user_data.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(user, key, value)
    session.add(user)
    session.commit()
    return user

def delete_user_by_id(user_id: int, session: Session):
    user = session.exec(select(User).where(User.id == user_id)).one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    session.delete(user)
    session.commit()
    return {"message": "User deleted successfully"}