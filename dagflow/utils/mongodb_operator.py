# -*- coding: utf-8 -*-
__author__ = 'godq'
from pymongo import MongoClient
import gridfs


class MongoOperator(object):
    def __init__(self, url, db_name="dagflow", replicaset=None):
        self._client = MongoClient(url, connect=False, replicaset=replicaset)
        self._db_name = db_name
        self._db = None
        self._fs = None
        self._switch_db(self._db_name)

    def __enter__(self):
        self._switch_db(self._db_name)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.close()

    @property
    def client(self):
        return self._client

    @property
    def db(self):
        return self._db

    @property
    def fs(self):
        if not self._fs:
            self._fs = gridfs.GridFS(self._db, collection='dagflow_fs')

        return self._fs

    def add_file_to_mongo(self, file_path, target_path, **kwargs):
            with open(file_path, "r+b") as f:
                self.fs.put(f, filename=target_path, **kwargs)

    def get_file_from_mongo(self, search_fields):
        file = self.fs.find_one(search_fields)
        print(file)
        if not file:
            print("There is no this file")
            return None
        data = file.read()
        return data

    def _switch_db(self, db_name):
        self._db = self._client[db_name]
        self._fs = None

        return self._db


mongodb_client = None


def get_mongodb_client():
    global mongodb_client
    if mongodb_client:
        return mongodb_client

    from dagflow.config import Config
    mongodb_client = MongoOperator(**Config.repo_mongodb_url)
    return mongodb_client


if __name__ == '__main__':
    mongodb_client = get_mongodb_client()
    with mongodb_client as my_mongodb_client:
        db = my_mongodb_client.db
        db.dag_ref.insert({"name": "aaa", "_id": "aaa"})
        res = db.dag_ref.find()
        print(res)
        for i in res:
            print(i)

    with mongodb_client as my_mongodb_client:
        db = my_mongodb_client.db
        db.dag_ref.insert({"name": "bbb", "_id": "bbb"})
        res = db.dag_ref.find()
        print(res)
        for i in res:
            print(i)