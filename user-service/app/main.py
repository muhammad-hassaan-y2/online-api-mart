# main.py
from contextlib import asynccontextmanager
from typing import Annotated
from sqlmodel import Session, SQLModel
from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from jose import jwt
from datetime import datetime, timedelta
from typing import AsyncGenerator
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json

from app import settings
from app.db_engine import engine
from app.models.user_model import User, UserUpdate
from app.crud.user_crud import create_user, get_all_users, get_user_by_username, update_user_by_id, delete_user_by_id
from app.auth import authenticate_user, create_access_token, get_current_active_user
from app.deps import get_session, get_kafka_producer

def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)


async def consume_messages(topic, bootstrap_servers):
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="my-prodocct-consumer-group",
        # auto_offset_reset="earliest",
    )

    # Start the consumer.
    await consumer.start()
    try:
        async for message in consumer:
            user_data = json.loads(message.value.decode())
            with next(get_session()) as session:
                create_user(user_data=User(**user_data), session=session)
    finally:
        await consumer.stop()


# The first part of the function, before the yield, will
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("Creating table!")

    task = asyncio.create_task(consume_messages(
        settings.KAFKA_PRODUCT_TOPIC, 'broker:19092'))
    create_db_and_tables()
    yield


app = FastAPI(
    lifespan=lifespan,
    title="Hello World API with DB",
    version="0.0.1",
)



@app.get("/")
def read_root():
    return {"Hello": "Product Service"}


@app.post("/users", response_model=User)
async def create_user_endpoint(user: User, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):
    user_json = json.dumps(user.dict()).encode("utf-8")
    await producer.send_and_wait(settings.KAFKA_USER_TOPIC, user_json)
    return create_user(user_data=user, session=session)

@app.get("/users", response_model=list[User])
def get_all_users_endpoint(session: Annotated[Session, Depends(get_session)]):
    return get_all_users(session)

@app.get("/users/{user_id}", response_model=User)
def get_user_by_id_endpoint(user_id: int, session: Annotated[Session, Depends(get_session)]):
    return get_user_by_username(user_id=user_id, session=session)

@app.patch("/users/{user_id}", response_model=User)
def update_user_by_id_endpoint(user_id: int, user: UserUpdate, session: Annotated[Session, Depends(get_session)]):
    return update_user_by_id(user_id=user_id, user_data=user, session=session)

@app.delete("/users/{user_id}", response_model=dict)
def delete_user_by_id_endpoint(user_id: int, session: Annotated[Session, Depends(get_session)]):
    return delete_user_by_id(user_id=user_id, session=session)

@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()], session: Annotated[Session, Depends(get_session)]):
    user = authenticate_user(session, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": user.username}, expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer"}