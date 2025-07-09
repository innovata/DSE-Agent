# -*- coding: utf-8 -*-
# DSE(Data Science Experience) APIs 
# https://dsdoc.dsone.3ds.com/devdoccaa/3DEXPERIENCER2024x/en/DSDoc.htm?show=CAADataFactoryStudioWS/datafactorystudio_v1.htm
# REST API functions to interact with 3DS Server related to DSE 



import os, sys 
import pprint 
pp = pprint.PrettyPrinter(indent=2)


import requests 
from pymongo import MongoClient 
client = MongoClient()


import requests
import json
import base64
from pathlib import Path
from tqdm import tqdm 
import threading


from ipylib import ifile 



d = ifile.read_jsonfile(os.environ['CLM_AGENT_CREDENTIAL_PATH'])
sess = requests.Session()
sess.auth = (d['Agent ID'], d['Agent Password'])


TENANT_URI = os.environ['3DX_PLATFORM_TENANT_URI']
REST_API_URL = TENANT_URI + "/data-factory"


DB_NAME = os.environ["PROJECT_DB_NAME"]



def print_response(response):
    print("\n응답코드-->", response.status_code)
    if response.status_code > 399:
        print("\n\n응답코드가 2xx 또는 3xx 대역이 아닐 경우 아래와 같이 표시됩니다-->")
        pp.pprint(response.__dict__)
    return response




class Storages:

    _url = f"{REST_API_URL}/resources/v1/storage"

    def __init__(self):
        self.model = client[DB_NAME][self.__class__.__name__]

    # 스토리지 생성
    def create(
        self,
        stype:str, # 스토리지타입: "ObjectStorage", "IndexUnit" 
        name:str,
        description:str="",
        config:dict=None,
    ):

        if stype == 'ObjectStorage':
            config = config if config else {}
        elif stype == 'IndexUnit':
            # SGI 는 반드시 config.datamodel 필드를 추가
            config = config if config else {"datamodel": {}} 

        res = sess.post(
            self._url,
            json={
                "@class": stype,
                "name": name,
                "description": description,
                "resourceId": name,
                "config": config,
            }
        )
        return print_response(res)

    # 모든 스토리지 가져오기 & DB저장
    def get_n_save(self):
        res = sess.get(self._url)
        data = res.json()['cards']
        self.model.drop()
        self.model.insert_many(data)

    # 스토리지 검색-1
    def search_by_name(self, name:str, workspace_id:str="dw-global-000000-default"):
        res = sess.post(
            url=f"{self._url}/filter",
            json={
                "types": [
                    "IndexUnit",
                    "ObjectStorage"
                ],
                "top": 50,
                "skip": 0,
                "nameFilter": name,
                "workspaceId": workspace_id,
            }
        )
        return print_response(res)

    # 스토리지 검색-2
    def search_by_uuid(self, resource_uuid):
        res = sess.get(
            url=f"{self._url}/{resource_uuid}"
        )
        pass
    
    # 스토리지 삭제
    def delete(self, resource_uuid):
        res = sess.delete(
            url=f"{self._url}/{resource_uuid}"
        )

    # 스토리지 업데이트
    def update(self, resource_uuid):
        res = sess.put(
            url=f"{self._url}/{resource_uuid}",
            json={

            }
        )
    
    # 스토리지 Import
    def import_storage(self, resource_uuid):
        res = sess.post(
            url=f"{self._url}/import",
            json={

            }
        )

    # 스토리지 Export
    def export_storage(self, resource_uuid):
        res = sess.get(
            url=f"{self._url}/{resource_uuid}/export"
        )

    # 스토리지 비움
    def clean_storage(self, resource_uuid):
        res = sess.get(
            url=f"{self._url}/{resource_uuid}/clear"
        )



class ObjectStorage:

    # 스토리지 타입: ObjectStorageBucket 에 대한 REST API 

    _url = f"{REST_API_URL}/resources/v1/objectstorage"
    # dp-global-000000	
    # 0a28ee53-4def-4c92-ba75-87537d83185f	
    # dw-global-000000-default	
    # TestDATASET-01	
    # jle69_gmail	
    # Storage	
    # ObjectStorage

    def __init__(self, resourceUUID="0a28ee53-4def-4c92-ba75-87537d83185f"):
        self.resourceUUID = resourceUUID

    def multicheckin(self):
        res = sess.post(
            url=f"{self._url}/{self.resourceUUID}/multicheckin", 
            json={
                'objects': [
                    {
                        "customAttribute": "binary",
                        "correlationId": "correlation01",
                        "id": "test_rest/file1.json"
                    },
                ]
            } 
        )

    def upload(self, file:str, pbar:object=None):
        with open(file, 'rb') as f:
            file_content = f.read()
            encoded_content = base64.b64encode(file_content).decode('utf-8')

        res = sess.post(
            url=f"{self._url}/{self.resourceUUID}/upload", 
            json={
                "files": [
                    {
                        "id": str(Path(file).as_posix()), # DFS 스토리지상의 파일 절대경로
                        "filename": os.path.basename(file),
                        "content": encoded_content,
                        "filesize": os.path.getsize(file)
                    }
                ]
            } 
        )
        if pbar:
            pbar.update(1)

    def upload_many(self, files:list):
        with tqdm(total=len(files), desc="DFS에 파일 업로드") as pbar:
            threads = []
            for file in files:
                th = threading.Thread(target=self.upload, args=(file, pbar))
                th.start()
                threads.append(th)

            for th in threads:
                th.join()

    def commit(self):
        pass 



def data_transform_01(action:str, data:list)->list:
    jsondata = []
    for d in data:
        jsondata.append({
            "action": action,
            "item": d
        })
    return jsondata 


class SemanticGraphIndex:

    _url = f"{REST_API_URL}/resources/v1/indexunit"

    def __init__(self):
        pass

    def ingest(self, resourceUUID:str, data:list):
        res = sess.post(
            url=f"{self._url}/{resourceUUID}/ingest",
            json=data_transform_01("AddOrReplaceItem", data)
        )
        return print_response(res) 

    def notification(self, resourceUUID):
        res = sess.get(
            url=f"{self._url}/{resourceUUID}/notification",
        )
        return print_response(res) 

    def validateItemsEvent(self, resourceUUID):
        res = sess.post(
            url=f"{self._url}/{resourceUUID}/validateItemsEvent",
            json=["null"]
        )
        return print_response(res) 

    def get_uri(self, resourceUUID):
        res = sess.get(
            url=f"{self._url}/{resourceUUID}/uri",
        )
        return print_response(res)

    def class_count(self, resourceUUID:str, pkg_name:str, class_name_li:list):
        class_name_li = [f"{pkg_name}.{elem}" for elem in class_name_li]
        res = sess.get(
            url=f"{self._url}/{resourceUUID}/class/count",
            params={'classNameList': class_name_li, 'offset': 0, 'limit':10}
        )
        return print_response(res)

    def get_index(self, sgi_name:str):
        res = sess.get(
            url=f"{self._url}/name/{sgi_name}"
        )
        return print_response(res) 

    


class Pipeline:

    def __init__(self):
        pass


