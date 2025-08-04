from fastapi import FastAPI
import pandas as pd
from contextlib import asynccontextmanager
import logging

logger = logging.getLogger("uvicorn.error")

class EventStore:

    def __init__(self, max_events_per_user=10):

        self.events = {}
        self.max_events_per_user = max_events_per_user
        self._events_data = None

    def load(self, path):
        logger.info(f"Loading data, type: {type}")

        self._events_data = pd.read_parquet(path)

    def put(self, user_id, item_id):
        """
        Сохраняет событие
        """

        user_events = self.events.get(user_id, [])
        print(user_events)
        self.events[user_id] = [item_id] + user_events[: self.max_events_per_user]

    def get(self, user_id, k):
        """
        Возвращает события для пользователя
        """
        user_events = self.events.get(user_id, [])
        user_events.reverse()
        return user_events[:k]
    
   

events_store = EventStore()# ваш код здесь #


@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    events_store.load("events.par")

    logger.info("Ready!")
    # код ниже выполнится только один раз при остановке сервиса
    yield
    logger.info("Stopping")
    
# создаём приложение FastAPI
app = FastAPI(title="events", lifespan=lifespan)


@app.post("/put")
async def put(user_id: int, item_id: int):
    """
    Сохраняет событие для user_id, item_id
    """

    events_store.put(user_id, item_id)

    return {"result": "ok"}

@app.post("/get")
async def get(user_id: int, k: int = 10):
    """
    Возвращает список последних k событий для пользователя user_id
    """

    events = events_store.get(user_id, k)

    return {"events": events}