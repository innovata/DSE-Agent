# -*- coding: utf-8 -*-



import os 


from pymongo import MongoClient
client = MongoClient()






def print_db_error(e, **kwargs):
    # print(f"\nERROR | {e}", kwargs)
    pass 



class _Collection_:

    def __init__(self, coll_name:str=None):
        self.db_name = os.environ["PROJECT_DB_NAME"]
        if coll_name is None:
            coll_name = self.__class__.__name__
        self.coll_name = coll_name
        self.collection = client[self.db_name][self.coll_name]

    @property
    def name(self):
        return self.collection.name
    
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    def find(self, filter={}, proj={}, **kwargs):
        try:
            return self.collection.find(filter, proj, **kwargs)
        except Exception as e:
            print_db_error(e, filter=filter, proj=proj, **kwargs)
    
    def distinct(self, key, filter={}):
        try:
            return self.collection.distinct(key, filter=filter)
        except Exception as e:
            print_db_error(e, key=key, filter=filter)

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    def insert_one(self, doc:dict):
        try:
            return self.collection.insert_one(doc)
        except Exception as e:
            print_db_error(e)

    def insert_many(self, data:list):
        try:
            return self.collection.insert_many(data)
        except Exception as e:
            print_db_error(e)

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    
    def update_one(self, filter, update, upsert=False):
        try:
            return self.collection.update_one(filter, update, upsert=upsert)
        except Exception as e:
            print_db_error(e, filter=filter, update=update, upsert=upsert)

    def update_many(self, filter, update, upsert=False):
        try:
            return self.collection.update_many(filter, update, upsert=upsert)
        except Exception as e:
            print_db_error(e, filter=filter, update=update, upsert=upsert)

    # 자주 사용하는 함수라서 커스텀한 메소드 
    def upsert_many(self, data, filter={}):
        try:
            for d in data:
                rv = self.collection.update_many(
                    filter,
                    {'$set': d},
                    upsert=True
                )
        except Exception as e:
            print_db_error(e, data=data, filter=filter)

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    def drop(self):
        try:
            return self.collection.drop()
        except Exception as e:
            print_db_error(e)

    def delete_one(self, filter):
        try:
            return self.collection.delete_one(filter)
        except Exception as e:
            print_db_error(e)

    def delete_many(self, filter):
        try:
            return self.collection.delete_many(filter)
        except Exception as e:
            print_db_error(e)





class Storages(_Collection_):

    def __init__(self):
        super().__init__()





