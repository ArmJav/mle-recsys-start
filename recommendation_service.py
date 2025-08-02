import logging

from fastapi import FastAPI
from contextlib import asynccontextmanager
from handle import Recommendations

rec_store = Recommendations()
logger = logging.getLogger("uvicorn.error")
rec_store.load(
    "personal",
    'final_recommendations_feat.parquet',
    columns=["user_id", "item_id", "rank"],
)
rec_store.load(
    "default",
    'top_recs.parquet',
    columns=["item_id", "rank"],
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    logger.info("Starting")
    yield
    # этот код выполнится только один раз при остановке сервиса
    logger.info("Stopping")
    
# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)

@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """
    rec_store.load(
        "personal",
        'final_recommendations_feat.parquet',
        columns=["user_id", "item_id", "rank"],
    )
    rec_store.load(
        "default",
        'top_recs.parquet',
        columns=["item_id", "rank"],
    )
    recs = rec_store.get(user_id, k)

    return {"recs": recs}