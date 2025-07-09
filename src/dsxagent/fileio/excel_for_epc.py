# -*- coding: utf-8 -*-

# Excel,CSV, JSON 등등 파일에 대한 읽기, 쓰기 라이브러리 
# Data Factory Studio / Object Storage 에 올리는 파일포멧(CSV, JSON)으로 변환하는 기능 관련 모듈 
# EPC Platform PoC 때 개발했던 모듈





import os  
import re 
import pprint 
pp = pprint.PrettyPrinter(indent=2)
import json 
from pathlib import WindowsPath

import pandas as pd


from fileio.core import *



################################################################
# 엑셀 파일
################################################################

class ExcelSheet:
    # xslx_file: 엑셀파일의 파일명 포함 전체경로
    # sheet_name: 엑셀파일 원본 시트명
    # alias_name: DFS에 업로드하기 위해 반드시 필요한 영문 시트명. 데이터모델링 시 클래스명으로 동일하게 사용하길 권장

    def __init__(self, xslx_file, sheet_name, alias_name=None):
        self.filepath = xslx_file
        
        # 시트명으로 모델명 설정
        self.name = sheet_name
        if check_has_korean(sheet_name):
            if alias_name is None:
                self.modelName = None
            else:
                if check_has_korean(alias_name):
                    self.modelName = None
                else:
                    self.modelName = alias_name
        else:
            self.name = sheet_name 
            self.modelName = sheet_name 

        if self.modelName is None:
            print("""
            WARNING | 한글 시트명을 반드시 영문으로 바꿔야 하는 이유:
            [1] DFS에 업로드하기 위해서 파일명 포함 전체 경로 문자열이 영문이 아니면 Pipeline을 사용할 수 없다.
            [2] DFS에서 데이터 모델링 시 클래스명은 반드시 영문이어야 한다. 클래스명은 '파이썬 클래스' 작명 규칙에 따라 작성해야한다.
            """)

    # 엑셀시트의 테이블을 데이터프레임으로 읽어온다
    def load_data(self):
        df = pd.read_excel(self.filepath, sheet_name=self.name)
        return df.dropna(axis=0, how='all')

    # DFS Data Modeling이 요구하는 형태로 스키마 파일구조 생성
    def gen_schema(self):
        df = self.load_data()
        df = restruct_dataframe(df)
        schema = generate_schema(df)
        return schema
    
    def parse_data(self, schema_filepath=None):
        df = self.load_data()
        if schema_filepath is None:
            # 자동으로 변환
            df = normalize_dataframe(df)
        else:
            # 사용자 지정 스키마 기준으로 변환
            # 컬럼명만 변환한다
            schema = read_json(schema_filepath)
            # pp.pprint(schema)
            columns = {}
            for col, dic in schema.items():
                columns.update({col: dic['Column Name']})
            # pp.pprint(columns)

            df = restruct_dataframe(df)
            df = df.rename(columns=columns)

        return df



class ExcelFile:

    def __init__(self, xslx_file, sheet_trsl_file=None):
        # 파일경로 + 파일명 + 확장자
        self.filepath = xslx_file

        # 실제 파일 경로
        self.path = os.path.dirname(xslx_file)
        # 실제 파일 (확장자 포함)
        self.file = os.path.basename(xslx_file)
        # DFS / Data Model / Package Name 으로 사용할 변수명 
        root, ext = os.path.splitext(self.file)
        self.filename = root
        self.pkg_name = normalize_pkgname(root)


        # 파일 CONFIG 생성
        self.make_dir()
        self._load_config()


    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # 파일 CONFIG 생성
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    # 실제 파일명으로 신규 폴더 생성
    def make_dir(self):
        create_dir_by_filename(self.filepath)
        self.workdir = os.path.join(self.path, self.filename)

    def _load_config(self):
        self._config = read_json(os.path.join(self.workdir, 'configuration.json'))

    def build_config(self):
        d = {
            'SheetNames': {},
            'SheetData': {},
        }
        write_json(obj, filepath)

    # 시트명 검사 
    def _inspect(self):
        return 
        # self.ef = pd.ExcelFile(xslx_file)
        # self.sheet_names = self.ef.sheet_names
        # print('원본시트명 리스트:', self.sheet_names)
        # if sheet_trsl_file is None:
        #     for name in self.sheet_names:
        #         if check_has_korean(name):
        #             print("ERROR | 시트명 한/영 변환 JSON파일을 파라미터 'sheet_trsl_file'로 입력하시오.")
        #             raise 
        # else:
        #     # 시트명 한/영 변환 메타데이터
        #     self.sheet_trsl = read_json(sheet_trsl_file)
        #     for name in self.sheet_names:
        #         if check_has_korean(name):
        #             if name in self.sheet_trsl:
        #                 pass 
        #             else:
        #                 print("ERROR | 시트명 한/영 변환 JSON파일을 수정하시오.")
        #                 raise 
        #         else:
        #             self.sheet_trsl.update({name: normalize_classname(name)})

    # ExcelSheet 객체를 리턴
    def get_sheet(self, sheet_name):
        def __get_alias__(sheet_name):
            if sheet_name in self.sheet_trsl:
                return self.sheet_trsl[sheet_name]
            else:
                return sheet_name
            
        return ExcelSheet(self.filepath, sheet_name, alias_name=__get_alias__(sheet_name))


    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # 스키마 파일 생성
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    def write_schema_file(self, sheet_name):
        sheet = self.get_sheet(sheet_name)
        schema = sheet.gen_schema()

        json_file = os.path.join(self.workdir, 'SCHEMA_' + sheet_name + '.json')
        # print('스키마파일경로:', json_file)
        write_json(schema, json_file)

    # 각 시트에 대해 스키마 JSON 파일을 생성
    def generate_all_schemas(self):
        self.make_dir()
        for sheet_name in self.sheet_names:
            self.write_schema_file(sheet_name)
        print('스키마 JSON 파일 생성완료.')
    

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # CSV 파일 생성
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    def write_csv_file(self, sheet_name):
        sheet = self.get_sheet(sheet_name)
        df = sheet.parse_data()
        csv_file = os.path.join(self.workdir, sheet.modelName + '.csv')
        save_as_csv(csv_file, df)

    # 모든시트를 CSV 파일로 저장
    def extract_all_sheets(self):
        self.make_dir()
        for sheet_name in self.sheet_names:
            self.write_csv_file(sheet_name)
        print('엑셀파일의 모든 시트를 CSV로 변환완료.')


    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # DFS 경로 생성
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    # DFS/Object Storage 에 파일을 올릴 때 사용할 경로
    def gen_rawdata_storage_path(self, project_name, provider_name):
        return f"{project_name}/Rawdata/{provider_name}/{self.pkg_name}/"
    
    def gen_demodata_storage_path(self, project_name, ftype='CSV'):
        return f"{project_name}/DemoData/{ftype}/{self.pkg_name}/"

    



    


