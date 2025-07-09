# -*- coding: utf-8 -*-
import os 
import math 


import pandas as pd
import numpy as np
from pymongo import MongoClient
client = MongoClient()




# def get_schema_from_doc(doc):
#     # return {k: type(v) for k,v in doc.items()}
#     schema = {}
#     for k,v in doc.items():
#         if type(v) == str:



# 컬럼당 유니크 밸류
def view_unique_values(df):
    columns = list(df.columns)
    print()
    data = []
    for c in columns:
        vals = list(df[c].unique())
        data.append({'column': c, 'unq_len': len(vals), 'sample': vals[:50]})

        if len(vals) > 100:
            print(f'{c} --> {len(vals)} | {vals[:100]}')
        else:
            print(f'{c} --> {len(vals)} | {vals}')
    
    return pd.DataFrame(data).sort_values('unq_len', ascending=False).reset_index(drop=True)


# 컬럼당 유니크 밸류
def analyze_column_unique_values(dbName, collName, filter=None, printout=False, on_cols=None):
    # 분석대상테이블의 컬럼리스트 
    coll = client[dbName][collName]
    
    # 프로젝션 셋업
    if on_cols is None:
        cursor = coll.find({}, {'_id':0}, limit=1)
        columns = list(list(cursor)[0])
        # print(columns)
    elif isinstance(on_cols, list):
        columns = on_cols.copy()
    else:
        raise 

    # 필터 셋업
    filter = {} if filter is None else filter

    # 분석데이터 빌드
    data = []
    for i, c in enumerate(columns, start=1):
        try:
            filter.update({c: {'$nin': [None, math.nan]}})
            vals = coll.distinct(c, filter=filter)
        except Exception as e:
            print(f"ERROR | {c} | {e}")
            cursor = coll.find(filter, {'_id':0, c:1})
            df = pd.DataFrame(list(cursor), dtype='str')
            vals = list(df[c].unique())
        finally:
            # print(vals)
            data.append({
                'column': c,
                'unq_cnt': len(vals),
                'unq_values': vals 
            })
    
    return pd.DataFrame(data)


def get_schema_from_db(dbName, collName, filter=None):
    db = client[dbName]
    p = {'_id':0}
    cursor = db[collName].find(filter, p, limit=1)
    data = list(cursor)
    df = pd.DataFrame(data, dtype='str')
    return list(df.columns)






def analyze_column_dtype(values):

    return 



