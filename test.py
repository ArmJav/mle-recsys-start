# from handle import Recommendations

# rec_store = Recommendations()

# rec_store.load(
#     "personal",
#     'final_recommendations_feat.parquet',
#     columns=["user_id", "item_id", "rank"],
# )
# rec_store.load(
#     "default",
#     'top_recs.parquet',
#     columns=["item_id", "rank"],
# )

# res = rec_store.get(user_id=1049126, k=5)
# print(res)
import requests

recommendations_url = "http://127.0.0.1:8000"
features_store_url = "http://127.0.0.1:8010"
events_store_url = "http://127.0.0.1:8020" 


headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

user_id = 1291250
event_item_ids =  [7144, 16299, 5907, 18135]

for event_item_id in event_item_ids:
    resp = requests.post(events_store_url + "/put", 
                         headers=headers, 
                         params={"user_id": user_id, "item_id": event_item_id})
                         
params = {"user_id": 1291250, 'k': 10}
resp_offline = requests.post(recommendations_url + "/recommendations_offline", headers=headers, params=params)
resp_online = requests.post(recommendations_url + "/recommendations_online", headers=headers, params=params)
resp_blended = requests.post(recommendations_url + "/recommendations", headers=headers, params=params)

recs_offline = resp_offline.json()["recs"]
recs_online = resp_online.json()["recs"]
recs_blended = resp_blended.json()["recs"]

print(recs_offline)
print(recs_online)
print(recs_blended)

