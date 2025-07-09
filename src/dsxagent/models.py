# -*- coding: utf-8 -*-
# Data Factory Studio / SGI Data Modeling 기능 



import os, sys 
import pprint 
pp = pprint.PrettyPrinter(indent=2)
import json 




from dsxagent import restapi 



class SemanticGraphIndex:

    def __init__(self):
        self.schema = None 

    def create_storage(
            self,
            name:str="TestSGI-02", # SGI명 
            description:str="REST API 테스트로 생성한 파일스토리지. 삭제할 것",
            data_file:str=None, # 저장할 데이터가 있는 파일 절대경로 
        ):
        if data_file:
            autoschema = AutoSchema()
            config = autoschema.gen_schema(data_file)

            if config:
                api = restapi.Storages()
                api.create(
                    stype="IndexUnit",
                    name=name,
                    description=description,
                    config=config
                )
            else:
                print("\nERROR | 스키마를 먼저 정의하세요.")

    # 클라우드상에 정의된 config 다운로드 
    def export_config(self):
        pass 


class AutoGenSchema:

    def __init__(self):
        pass

    def gen(self, file):
        with open(file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        doc = data[0]
        _class = doc.pop('class')
        pkg_name, cls_name = _class.split('.')
        sgicls = SGIClass(pkg_name, cls_name)
        for k,v in doc.items():
            sgicls.add_attr(k, "String")
        official_class = sgicls.gen_class_definition_for_config()

        schema = {'datamodel': {"classes": [official_class]}}
        pp.pprint(schema)
        return schema 
    
    def parse_json_data(self, data):
        keys = list(data[0])
        values = {k: [] for k in keys}
        for d in data:
            for i in range(len(keys)):
                key = keys[0]
                val = d[key]
                values[key].append(str(type(val)))
        
        # Counter 객체사용 
        # 가장 많은 Dtype 을 선택해서, 확정
        # {key: dtype} 형식으로 반환 
        



class SGIClass:
    # 데이터구조 
    {
        "name": "TestClass-01",
        "parents": [],
        "pkg": "pkg",
        "attributes": [
            {
                "name": "title",
                "type": {
                    "dataType": "String",
                    "dataStructure": "Singleton"
                }
            },
            {
                "name": "_text",
                "type": {
                    "dataType": "String",
                    "dataStructure": "Singleton"
                },
                "annotation": {}
            }
        ]
    }

    def __init__(self, pkg_name, cls_name):
        self.pkg_name = pkg_name 
        self.cls_name = cls_name 
        self.attrs = []

    def add_attr(self, name, dtype):
        self.attrs.append((name, dtype))

    # REST API로 클래스 생성을 요청할 데이터포멧으로 구성
    def gen_class_definition_for_config(self):
        attrs = []
        for name, dtype in self.attrs:
            attrs.append({
                "name": name,
                "type": {
                    "dataType": dtype,
                    "dataStructure": "Singleton"
                }
            })
        
        return {
            "name": self.cls_name,
            "parents": [],
            "pkg": self.pkg_name,
            "attributes": attrs
        }
    
    # 파일을 읽어들여 스키마를 구성한다
    def gen_schema(self, file):
        ags = AutoGenSchema()

        pass 
