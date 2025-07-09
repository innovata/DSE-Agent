# -*- coding: utf-8 -*-
# Data Factory Studio / SGI Data Modeling 관련 기능 



import os 
import pprint 
pp = pprint.PrettyPrinter(indent=2)

import pandas as pd
import numpy as np


from dsxagent import dflib 
import fileio


# EXCEL, CSV 데이터 파일로부터 데이터스키마를 정의하고,
# Data Factory Studio/Data Model 에 Import 할 json 파일을 자동생성.
"""
"resourceId" : "JLE69_EPCPlatformPoc",
"resourcePath" : "IndexUnit/EPCPlatformPoc.json",
패키지명 - 클래스명 리스트 정의 필요 

"""
def generate_datamodel_json(json_file=None):
    "J:\다른 컴퓨터\My Computer\Data\DSE_CONFIG\Data_Factory_Studio\SGI\JLE69_EPCPlatformPoc__v1.json"
    # 기존 JSON-CONFIG 파일이 있으면 업데이트한다
    # 없으면 새로 만든다
    if json_file is None:
        pass 
    else:
        js = fileio.ifile.read_jsonfile(json_file)
        js['storage']['config']['datamodel']['classes']
    return 


def interpret_column_dtype(df):
    srs = df.dtypes
    result = {}
    for i, v in zip(srs.index, srs.values):
        # print(i, v, type(v))
        if isinstance(v, np.dtypes.Float64DType):
            v = 'Float'
        elif isinstance(v, np.dtypes.Int64DType):
            v = 'Integer'
        elif isinstance(v, np.dtypes.ObjectDType):
            v = 'String'
        result.update({i: v})
    return result 


# 수작업으로 DPS/Data Model 에서 클래스를 생성하기 위한 보조 텍스트 파일을 생성 
def write_vertical_columns_textfile(csv_file):
    df = pd.read_csv(csv_file)
    cols = list(df.columns)
    root, ext = os.path.splitext(csv_file)
    txt_file = root + '.txt'
    print('\n\n')
    print(f'입력파일: {csv_file}')
    print(f'컬럼 개수: {len(cols)}')
    with open(txt_file, 'w', encoding='utf-8') as f:
        for c in cols:
            f.write(f"{c}\n")
        f.close()
    print(f'출력파일: {txt_file}')
    

def get_schema(csv_file):
    df = pd.read_csv(csv_file)
    schema = dflib.interpret_column_dtype(df)
    pp.pprint(schema)
    return schema 
    

def write_schema(csv_file):
    schema = get_schema(csv_file)
    data = []
    for k, v in schema.items():
        data.append({'column': k, 'dtype': v})
    df = pd.DataFrame(data).sort_values(['column']).reset_index(drop=True)
    print(df)

    path = os.path.dirname(csv_file)
    filename = os.path.basename(csv_file)
    sch_file = os.path.join(path, f"_Schema_{filename}")
    print('스키마파일:', sch_file)
    fileio.save_as_csv(sch_file, df)



