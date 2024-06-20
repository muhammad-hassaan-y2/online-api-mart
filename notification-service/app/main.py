# main.py
from contextlib import asynccontextmanager
from typing import Annotated
from sqlmodel import Session, SQLModel
from fastapi import FastAPI, Depends, HTTPException
from typing import AsyncGenerator
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json

from app import settings
from app.db_engine import engine
from app.models.notification_model import Notification, NotificationUpdate
from app.crud.notification_crud import create_notification, get_all_notifications, get_notification_by_id, update_notification_by_id, delete_notification_by_id
from app.deps import get_session, get_kafka_produce

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
        # Continuously listen for messages.
        async for message in consumer:
            print("RAW")
            print(f"Received message on topic {message.topic}")

            product_data = json.loads(message.value.decode())
            print("TYPE", (type(product_data)))
            print(f"Product Data {product_data}")

            with next(get_session()) as session:
                print("SAVING DATA TO DATABSE")
                db_insert_product = add_new_product(
                    product_data=Product(**product_data), session=session)
                print("DB_INSERT_PRODUCT", db_insert_product)

            # Here you can add code to process each message.
            # Example: parse the message, store it in a database, etc.
    finally:
        # Ensure to close the consumer when done.
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


@app.post("/notifications", response_model=Notification)
async def create_notification_endpoint(notification: Notification, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):
    notification_json = json.dumps(notification.dict()).encode("utf-8")
    await producer.send_and_wait(settings.KAFKA_NOTIFICATION_TOPIC, notification_json)
    return create_notification(notification_data=notification, session=session)

@app.get("/notifications", response_model=list[Notification])
def get_all_notifications_endpoint(session: Annotated[Session, Depends(get_session)]):
    return get_all_notifications(session)

@app.get("/notifications/{notification_id}", response_model=Notification)
def get_notification_by_id_endpoint(notification_id: int, session: Annotated[Session, Depends(get_session)]):
    return get_notification_by_id(notification_id=notification_id, session=session)

@app.patch("/notifications/{notification_id}", response_model=Notification)
def update_notification_by_id_endpoint(notification_id: int, notification: NotificationUpdate, session: Annotated[Session, Depends(get_session)]):
    return update_notification_by_id(notification_id=notification_id, notification_data=notification, session=session)

@app.delete("/notifications/{notification_id}", response_model=dict)
def delete_notification_by_id_endpoint(notification_id: int, session: Annotated[Session, Depends(get_session)]):
    return delete_notification_by_id(notification_id=notification_id, session=session)