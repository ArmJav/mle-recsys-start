import logging

from fastapi import FastAPI
from contextlib import asynccontextmanager
from handle import Recommendations
import requests

rec_store = Recommendations()
logger = logging.getLogger("uvicorn.error")

features_store_url = "http://127.0.0.1:8010"
events_store_url = "http://127.0.0.1:8020" 

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    logger.info("Starting")
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

    recs_offline = await recommendations_offline(user_id, k)
    recs_online = await recommendations_online(user_id, k)

    recs_offline = recs_offline["recs"]
    recs_online = recs_online["recs"]

    recs_blended = []

    min_length = min(len(recs_offline), len(recs_online))
    # чередуем элементы из списков, пока позволяет минимальная длина
    for i in range(min_length):
        if i % 2 == 0:
            recs_blended.append(recs_offline[i])  # чётные позиции
        else:
            recs_blended.append(recs_online[i])   # нечётные позиции
    print(recs_blended)
    # добавляем оставшиеся элементы в конец
    recs_blended.extend(recs_online[min_length:])
    recs_blended.extend(recs_offline[min_length:])

    # удаляем дубликаты
    recs_blended = dedup_ids(recs_blended)
    recs_blended = recs_blended[:k]
        # оставляем только первые k рекомендаций
        # ваш код здесь #

    return {"recs": recs_blended}

@app.post("/recommendations_offline")
async def recommendations_offline(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """
    recs = rec_store.get(user_id, k)

    return {"recs": recs}

def dedup_ids(ids):
    """
    Дедублицирует список идентификаторов, оставляя только первое вхождение
    """
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]

    return ids

@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
    """

    headers = {"Content-type": "application/json", "Accept": "text/plain"}

    # получаем последнее событие пользователя
    params = {"user_id": user_id, "k": 3}
    resp = requests.post(events_store_url + "/get", headers=headers, params=params)
    events = resp.json()
    events = events["events"]

    # получаем список похожих объектов
    items = []
    scores = []
    for item_id in events:
        # для каждого item_id получаем список похожих в item_similar_items

        params = {"item_id": item_id, "k": 1}
        resp = requests.post(features_store_url +"/similar_items", headers=headers, params=params)
        if resp.status_code == 200:
            item_similar_items = resp.json()
        else:
            item_similar_items = None
            print(f"status code: {resp.status_code}")
        
        items += item_similar_items["item_id_2"]
        scores += item_similar_items["score"]
    # сортируем похожие объекты по scores в убывающем порядке
    # для старта это приемлемый подход
    combined = list(zip(items, scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]

    recs = dedup_ids(combined)

    return {"recs": recs[:k]}