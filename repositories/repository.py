from pymongo import MongoClient

import json

class Repository():
    def __init__(self) -> None:
        client = MongoClient("localhost", 27017, maxPoolSize=50)
        self.db = client.gpo_tender_db
        self.structures = self.db.structures
        self.notifications = self.db.notifications

    def save_to_json(self) -> None:
        structures_json = self.get_structures_list()
        notifications_json = self.get_notifications_list()

        with open("structures.json", "w", encoding="utf-8") as file:
            json.dump(structures_json, file, ensure_ascii=False)

        with open("notifications.json", "w", encoding="utf-8") as file:
            json.dump(notifications_json, file, ensure_ascii=False)

    def get_structures_list(self) -> list:
        result = []

        for item in self.structures.find():
            del item["_id"]
            result.append(item)

        return result

    def get_notifications_list(self) -> list:
        result = []
        index = 0
        notifications = self.notifications.find()

        for item in notifications:
            index += 1
            del item["_id"]
            result.append(item)
            if index % 200 == 0:
                print(f"{index / len(notifications) * 100} %")

        return result