# -*- coding: utf-8 -*-
# DSE(Data Science Experience) APIs 
# https://dsdoc.dsone.3ds.com/devdoccaa/3DEXPERIENCER2024x/en/DSDoc.htm?show=CAADataFactoryStudioWS/datafactorystudio_v1.htm
# REST API functions to interact with 3DS Server related to DSE 



import os, sys 


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



class Storages:

    url = f"{REST_API_URL}/resources/v1/storage"

    def __init__(self):
        self.model = client[DB_NAME][self.__class__.__name__]

    # 모든 스토리지 가져오기 & DB저장
    def get_n_save(self):
        res = sess.get(self.url)
        data = res.json()['cards']
        self.model.drop()
        self.model.insert_many(data)

    # 스토리지 검색-1
    def search_by_name(self, name:str, workspace_id:str="dw-global-000000-default"):
        res = sess.post(
            url=f"{self.url}/filter",
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

    # 스토리지 검색-2
    def search_by_uuid(self, resource_uuid):
        res = sess.get(
            url=f"{self.url}/{resource_uuid}"
        )
        pass
    
    # 스토리지 삭제
    def delete(self, resource_uuid):
        res = sess.delete(
            url=f"{self.url}/{resource_uuid}"
        )

    # 스토리지 업데이트
    def update(self, resource_uuid):
        res = sess.put(
            url=f"{self.url}/{resource_uuid}",
            json={

            }
        )
    
    # 스토리지 Import
    def import_storage(self, resource_uuid):
        res = sess.post(
            url=f"{self.url}/import",
            json={

            }
        )

    # 스토리지 Export
    def export_storage(self, resource_uuid):
        res = sess.get(
            url=f"{self.url}/{resource_uuid}/export"
        )

    # 스토리지 비움
    def clean_storage(self, resource_uuid):
        res = sess.get(
            url=f"{self.url}/{resource_uuid}/clear"
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


class SemanticGraphIndex:

    def __init__(self):
        pass


class Pipeline:

    def __init__(self):
        pass


