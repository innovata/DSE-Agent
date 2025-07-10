# -*- coding: utf-8 -*-
# Data Factory Studio / SGI Data Modeling 기능 



import os, sys 
import pprint 
pp = pprint.PrettyPrinter(indent=2)
import json 




from dsxagent import models, restapi 




############################################################
# Object Storage
############################################################




############################################################
# Semantic Graph Index Unit 
############################################################

class SemanticGraphIndex:

    def __init__(self):
        self.schema = None 

    def create_storage(
            self,
            name:str, # SGI명 
            description:str="",
            args:list=None, # 저장할 데이터가 있는 파일 절대경로 | (pkg_name, cls_name, filepath)
        ):
        if args:
            classes = []
            for pkg_name, cls_name, data_file in args:
                cls = SGIClass(sgi_name=name, pkg_name=pkg_name, cls_name=cls_name)
                cls.ingest_file(data_file)
                if cls._schema:
                    classes.append(cls._schema)

            print("\nClasses-->")
            pp.pprint(classes)
            
            if len(classes) > 0:
                api = restapi.Storages()
                res = api.create(
                    stype="IndexUnit",
                    name=name,
                    description=description,
                    config={'datamodel': {"classes": classes}}
                )
                restapi.get_n_save_all_storages()
            else:
                print("\nERROR | 데이터 모델링할 클래스가 없습니다.")

    # 클라우드상에 정의된 config 다운로드 
    def export_config(self):
        pass 
   

from ipylib.dtype import TypeDetector, analyze_jsondata_column_types, convert_jsondata_with_types
from ipylib import ifile 

def read_file(file)-> list:
    root, ext = os.path.splitext(file)
    if ext == ".csv":
        return ifile.read_csvfile(file)
    elif ext == ".json":
        return ifile.read_jsonfile(file)




def extract_dtypes_from_json_data(data)-> dict:
    keys = list(data[0])
    result = {k: [] for k in keys}
    for d in data:
        for k,v in d.items():
            result[k].append(type(v))
    # result 

    # Counter 객체사용 
    # 가장 많은 Dtype 을 선택해서, 확정
    # {key: dtype} 형식으로 반환 
    from collections import Counter
    dtypes = {}
    for k, li in result.items():
        counter = Counter(li)
        _most_dtype = counter.most_common(1)[0][0]
        # print(_most_dtype)
        dtypes.update({k: _most_dtype})
        # print(dict(counter))
    return dtypes 
        

class SGIClass:
    # config 데이터구조 
    {
        "name": "TestClass-01",
        "parents": [],
        "pkg": "TestPackage",
        "attributes": [
            {
                "name": "title",
                "type": {
                    "dataType": "String",
                    "dataStructure": "Singleton"
                },
                "annotation": {}
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

    def __init__(self, sgi_name, pkg_name, cls_name=None):
        self.sgi_name = sgi_name
        self.pkg_name = pkg_name 
        self.cls_name = cls_name 
        self._attrs = {}
        self._data = None 

    # 로컬파일을 읽어들인 후, 스키마를 분석하고, 데이터 컬럼명을 청소한 후, 객체내부에 보관한다.
    def ingest_file(self, file):

        # 객체생성 시 클래스명을 입력하지 않았다면, 파일명으로 자동으로 셋업 
        if not self.cls_name:
            root, ext = os.path.splitext(file)
            self.cls_name = os.path.basename(root)

        # 파일 읽기
        rawdata = read_file(file)
        # 데이터 청소 
        translator = Translator()
        data = []
        for d in rawdata:
            doc = {}
            for k,v in d.items():
                # 컬럼명 청소 
                k = translator.translate_korean_to_english(k)
                k = translator.change_fixed_column_name(k)
                if v is None:
                    v = ""
                doc.update({k: v})
            data.append(doc)
        # 데이터 보관
        self._data = data

        # 데이터 스키마 파악
        dtypes = analyze_jsondata_column_types(data)
        pp.pprint(dtypes)
        # 데이터 스키마 커스텀 | 타입 용어 변환, 컬럼명 청소 
        handler = SGISchemaHandler()
        dtypes = handler.convert_dtypes_as_sgi_way(dtypes)
        pp.pprint(dtypes)
        # SGI클래스에 적용
        for column, dtype in dtypes.items():
            self.add_attr(column, dtype)
        # SGI 데이터모델링 전용 스키마 생성
        # REST API로 전송한 config 를 구성해서 반환 
        self.gen_schema()
        return self 

    def add_attr(self, name, dtype):
        self._attrs.update({name: dtype})

    # REST API로 클래스 생성을 요청할 데이터포멧으로 구성
    def gen_schema(self):
        if len(self._attrs) > 0:
            attrs = []
            for name, dtype in self._attrs.items():
                attrs.append({
                    "name": name,
                    "type": {
                        "dataType": dtype,
                        "dataStructure": "Singleton"
                    },
                    "annotation": {}
                })
            
            self._schema = {
                "name": self.cls_name,
                "parents": [],
                "pkg": self.pkg_name,
                "attributes": attrs
            }



class SGISchemaHandler:

    # _dtype_map = [
    #     (str, "String"),
    #     (int, "Integer"),
    #     (float, "Float"),
    #     (bool, "Boolean"),
    # ]

    _dtype_map = [
        ('str', "String"),
        ('int', "Integer"),
        ('float', "Float"),
        ('bool', "Boolean"),
        ('datetime', "DateTime"),
        ('date', "Date"),
    ]

    def __init__(self):
        pass

    def convert_dtypes_as_sgi_way(self, dtypes):
        result = {}
        for k,v in dtypes.items():
            for type1, type2 in self._dtype_map:
                if v is type1:
                    result.update({k: type2})
        return result 
    
    def clean_column_name(self, dtypes):
        result = {}
        translator = Translator()
        for k,v in dtypes.items():
            k = translator.translate_korean_to_english(k)
            k = translator.change_fixed_column_name(k)
            result.update({k: v})
        return result 

    




import argostranslate.package
import argostranslate.translate
import re

# 한글 컬럼명을 영문으로 바꾼다
class Translator:

    def is_korean(self, text):
        return bool(re.search(r'[\uAC00-\uD7A3]', text))

    def translate_korean_to_english(self, word):
        # 한국어인지 확인
        if not self.is_korean(word):
            return word
        
        try:
            # 번역 모델 설치 확인
            from_lang = "ko"
            to_lang = "en"
            
            # 사용 가능한 번역 패키지 확인 및 설치
            installed_languages = argostranslate.translate.get_installed_languages()
            language_codes = [lang.code for lang in installed_languages]
            
            if from_lang not in language_codes or to_lang not in language_codes:
                print("Installing Korean to English translation package...")
                argostranslate.package.update_package_index()
                available_packages = argostranslate.package.get_available_packages()
                package = next(
                    filter(
                        lambda x: x.from_code == from_lang and x.to_code == to_lang,
                        available_packages,
                    ),
                    None,
                )
                if package:
                    package.install()
                else:
                    return f"Error: No translation package found for {from_lang} to {to_lang}"
            
            # 번역 수행
            translated_text = argostranslate.translate.translate(word, from_lang, to_lang)
            return translated_text
        
        except Exception as e:
            return f"Error during translation: {str(e)}"
        
    def change_fixed_column_name(self, word):
        if word in ['date', 'datetime', 'text']:
            return f"_{word}"
        else:
            return word 





