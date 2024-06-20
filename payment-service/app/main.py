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
from app.models.payment_model import Payment, PaymentUpdate
from app.crud.payment_crud import create_payment, get_all_payments, get_payment_by_id, update_payment_by_id, delete_payment_by_id
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


@app.post("/payments", response_model=Payment)
async def create_payment_endpoint(payment: Payment, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):
    payment_json = json.dumps(payment.dict()).encode("utf-8")
    await producer.send_and_wait(settings.KAFKA_PAYMENT_TOPIC, payment_json)
    return create_payment(payment_data=payment, session=session)

@app.get("/payments", response_model=list[Payment])
def get_all_payments_endpoint(session: Annotated[Session, Depends(get_session)]):
    return get_all_payments(session)

@app.get("/payments/{payment_id}", response_model=Payment)
def get_payment_by_id_endpoint(payment_id: int, session: Annotated[Session, Depends(get_session)]):
    return get_payment_by_id(payment_id=payment_id, session=session)

@app.patch("/payments/{payment_id}", response_model=Payment)
def update_payment_by_id_endpoint(payment_id: int, payment: PaymentUpdate, session: Annotated[Session, Depends(get_session)]):
    return update_payment_by_id(payment_id=payment_id, payment_data=payment, session=session)

@app.delete("/payments/{payment_id}", response_model=dict)
def delete_payment_by_id_endpoint(payment_id: int, session: Annotated[Session, Depends(get_session)]):
    return delete_payment_by_id(payment_id=payment_id, session=session)