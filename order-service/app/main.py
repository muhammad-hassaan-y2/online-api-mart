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
from app.models.order_model import Order, OrderUpdate
from app.crud.order_crud import create_order, get_all_orders, get_order_by_id, update_order_by_id, delete_order_by_id
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
            order_data = json.loads(message.value.decode())
            with next(get_session()) as session:
                create_order(order_data=Order(**order_data), session=session)

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
    return {"Hello": "Order Service"}

@@app.post("/orders", response_model=Order)
async def create_order_endpoint(order: Order, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):
    order_json = json.dumps(order.dict()).encode("utf-8")
    await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, order_json)
    return create_order(order_data=order, session=session)

@app.get("/orders", response_model=list[Order])
def get_all_orders_endpoint(session: Annotated[Session, Depends(get_session)]):
    return get_all_orders(session)

@app.get("/orders/{order_id}", response_model=Order)
def get_order_by_id_endpoint(order_id: int, session: Annotated[Session, Depends(get_session)]):
    return get_order_by_id(order_id=order_id, session=session)

@app.patch("/orders/{order_id}", response_model=Order)
def update_order_by_id_endpoint(order_id: int, order: OrderUpdate, session: Annotated[Session, Depends(get_session)]):
    return update_order_by_id(order_id=order_id, order_data=order, session=session)

@app.delete("/orders/{order_id}", response_model=dict)
def delete_order_by_id_endpoint(order_id: int, session: Annotated[Session, Depends(get_session)]):
    return delete_order_by_id(order_id=order_id, session=session)