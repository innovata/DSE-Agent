# -*- coding: utf-8 -*-

# Innovata Data Parser 


import os  
import re 
import pprint 
pp = pprint.PrettyPrinter(indent=2)


import pandas as pd

 

################################################################
################################################################

def interpret_dtype(df):
    # df.info()
    # print(df.describe())
    srs = df.dtypes
    # print(srs)
    d = srs.to_dict()
    for k,v in d.items():
        v = v.__str__()
        if re.search('object', v) is not None:
            v = 'String'
        elif re.search('int', v) is not None:
            v = 'Integer'
        elif re.search('float', v) is not None:
            v = 'Float'
        elif re.search('date', v) is not None:
            v = 'DateTime'
        elif re.search('true|false', v) is not None:
            v = 'Boolean'
        else:
            print('ERROR: (개발오류)데이터타입을 추가하시오')
            raise 
        d.update({k: v})
    # pp.pprint(d)
    return d 


def generate_schema(df):
    dtypes = interpret_dtype(df)
    # return 

    columns = list(df.columns)
    schema = {}
    for c in columns:
        schema.update({
            c: {
                'Column Name': normalize_colname(c),
                'Data Type': dtypes[c],
                'Description': '',
            }
        })
    return schema 



def clean_dataframe(df):
    # ' ' 와 같은 값은 쓰레기 값이므로, None 처리
    df = df.applymap(lambda x: None if len(str(x).strip()) == 0 else x)
    # df = df.dropna(axis=1, how='all')
    df = df.dropna(axis=0, how='all')
    return df 


def normalize_filename(s):
    c = s.strip()
    # 날짜/시간 제거
    c = re.sub("\d{8}|\d{6}", repl='', string=c)
    c = re.sub("%", repl='pct', string=c)
    c = re.sub("['-\(\)\s\.]+", repl='_', string=c)
    c = re.sub("-", repl='_', string=c)
    c = re.sub("/", repl='_n_', string=c)
    c = re.sub("__", repl='_', string=c)
    # 양끝 조정
    c = re.sub("^_|_$", repl='', string=c)
    return c 


# 파일명을 DFS Storage & DataModel 에 사용할 패키지명으로 정규화
def normalize_pkgname(s):
    c = s.strip()
    # 날짜/시간 제거
    c = re.sub("\d{8}|\d{6}", repl='', string=c)
    c = re.sub("and", repl='_', string=c)
    c = re.sub("['-\(\)\s\.]+", repl='_', string=c)
    c = re.sub("-", repl='_', string=c)
    c = re.sub("__", repl='_', string=c)
    c = re.sub("^_|_$", repl='', string=c)
    # 맨마지막에 '_' 를 제거
    c = re.sub("_", repl='', string=c)
    return c 

def normalize_classname(s):
    words = re.split('\W', s)
    words = [w for w in words if len(w.strip()) > 0]
    words = [word.capitalize() for word in words]
    print(words)
    s = "".join(words)
    # 날짜/시간 제거
    s = re.sub("\d{8}|\d{6}", repl='', string=s)
    # 양끝 조정
    s = re.sub("^_|_$", repl='', string=s)
    print('클래스명:', s)
    return s 

def normalize_sheetname(s):
    return normalize_filename(s)


def normalize_colname(col):
    c = col.strip()
    # 엑셀시트 셀내부에서 엔터키 삭제 처리
    c = re.sub('_x000D_', repl='', string=c)
    # 빈칸, 특수기호 를 언더바 처리
    c = re.sub('[\s-]+', repl='_', string=c)
    c = re.sub("[\.']+", repl='', string=c)
    # 알파벳 문자열 처리
    c = c.lower()
    c = re.sub("^class$", repl='class_', string=c)
    c = re.sub("^matl$", repl='material', string=c)
    # 특수기호 문자화 처리
    c = re.sub("%", repl='pct', string=c)
    c = re.sub("/", repl='_n_', string=c)
    c = re.sub("_nat$", repl='', string=c)
    # 마무리 처리
    c = re.sub('[\(\)]', repl='_', string=c)
    c = re.sub("__", repl='_', string=c)
    c = re.sub("^_|_$", repl='', string=c)
    return c 


def get_normalized_colmap(cols):
    columns = {}
    for col in cols:
        columns.update({col: normalize_colname(col)})

    # print('\n정규화된 컬럼명 매핑:')
    # pp.pprint(columns)
    return columns


def has_2depth_columns(df):
    result = False 
    for c in list(df.columns):
        if re.search('[Uu]nnamed', c) is not None:
            result = True 
            break 
    # print('2뎁스 여부: ', result)
    return result 


def restruct_dataframe(df):
    if has_2depth_columns(df):
        df = restruct_2depth_columns(df)
    else:
        pass 
    return df 

def normalize_dataframe(df):
    df = restruct_dataframe(df)
    columns = get_normalized_colmap(list(df.columns))
    df = df.rename(columns=columns)
    return df 

# 2depth 컬럼 구조를 1depth 구조로 변경
# FROM DATAFRAME TO DATAFRAME
def restruct_2depth_columns(df):

    DEBUG_MODE = False

    ################################################################ PHASE-1
    # 데이터 분리 -> 컬럼Lv1, 컬럼Lv2, 값데이터

    def __view_2depth_columns__(cols_1, cols_2):
        if DEBUG_MODE:
            print('\n\n')
            print(cols_1)
            print(cols_2)
            print(len(cols_1), len(cols_2))

    # Lv1 컬럼명 추출 
    cols_1 = list(df.columns)

    # Lv2 컬럼명 추출 
    _df = df.iloc[0:1, :]
    _df = _df.fillna('')
    cols_2 = _df.values[0]
    __view_2depth_columns__(cols_1, cols_2)

    # 순수 값 데이터 추출 
    _df2 = df.iloc[1:, :]
    # print(_df2.head())
    pure_data = list(_df2.values)

    ################################################################ PHASE-2
    # 컬럼명 청소

    def __view_before_after_columns__(prev, next):
        if DEBUG_MODE:
            print('\n\n')
            print('전:', len(prev), prev)
            print('후:', len(next), next)

    # Unnamed 컬럼은 이전값으로 채운다
    def __fill_lv1_columns_(cols):
        prev = cols.copy()
        for i, c in enumerate(cols):
            if re.search('Unnamed', c) is not None:
                cols[i] = cols[i-1]

        __view_before_after_columns__(prev, cols)
        return cols
    
    def __clean_lv2_columns_(cols):
        prev = cols.copy()
        
        # 컬럼명 청소
        for i, c in enumerate(cols):
            if not isinstance(c, str):
                cols[i] = ''

        __view_before_after_columns__(prev, cols)
        return cols
    
    cols_1 = __fill_lv1_columns_(cols_1)
    cols_2 = __clean_lv2_columns_(cols_2)

    # print('LEVEL-1:', cols_1)
    # print('LEVEL-2:', cols_2)
    # return 

    ################################################################ PHASE-3
    # 다중구조 컬럼명 프레임 생성

    colname_frame = []
    for i, (c1, c2) in enumerate(zip(cols_1, cols_2)):
        # print([i, c1, c2])
        colname_frame.append([c1, c2])
    colname_frame = pd.DataFrame(colname_frame, columns=['c1', 'c2'])
    
    # 컬럼순서 유지를 위한 인덱스 설정
    colname_frame = colname_frame.reset_index(drop=False).rename(columns={'index': 'idx'})
    # return colname_frame

    for n, g in colname_frame.groupby('c1', sort=False):
        # print('-'*100)
        # print([n, len(g)])
        # print(g)
        if len(g) == 1:
            # print(g)
            idx = g.idx.values[0]
            # print('idx:', idx, type(idx))
            colname_frame.at[idx, 'c2'] = ''
            colname_frame.at[idx, 'c3'] = ''
            # break
        elif len(g) > 1:
            # 2중구조 컬럼일때 
            # print(g)
            # print(g.idx.values)
            vals = g.c2.values
            vals = [v.strip() for v in vals]
            # print(vals)

            len_1 = len(vals)
            len_2 = len(set(vals))
            if len_1 == len_2:
                # 2depth 컬럼명이 서로 다 다를때
                for idx in g.idx.values:
                    colname_frame.at[idx, 'c3'] = ''
            else:
                # 2depth 컬럼명에 일정한 규칙이 없을때 
                # print('시퀀싱 처리 필요.')
                # print(g)
                # print()
                # c2 값 채우기
                g['c2'] = g.c2.apply(lambda x: x if len(x.strip()) > 0 else None).ffill()
                # print(g)
                for d in g.to_dict('records'):
                    colname_frame.at[d['idx'], 'c2'] = d['c2']
                
                # c3 값 채우기
                for n2, g2 in g.groupby(['c1','c2'], sort=False):
                    # print('GROUP BY 2:', n2, len(g2))
                    idx_li = g2.idx.values
                    c3_vals = list(range(1, len(g2)+1))
                    # print(idx_li, c3_vals)
                    for idx, c3 in zip(idx_li, c3_vals):
                        colname_frame.at[idx, 'c3'] = c3 


    # colname_frame.info()
    # return colname_frame[:60]

    # 새로운 컬럼명 추가 
    for d in colname_frame.to_dict('records'):
        idx = d.pop('idx')
        vals = d.values()
        vals = [str(v) for v in vals if len(str(v)) > 0]
        colname = "_".join(vals)
        # print('컬러명:', colname)
        colname_frame.at[idx, 'column'] = colname

    # return colname_frame

    ################################################################ PHASE-4
    # 리빌딩된 데이터프레임 생성
    df = pd.DataFrame(pure_data, columns=list(colname_frame.column), dtype='str')
    # df.info()
    return df



# FROM DATAFRAME TO DATAFRAME
def normalize_2depth_columns(df):

    DEBUG_MODE = False

    ################################################################
    # 데이터 분리 -> 컬럼Lv1, 컬럼Lv2, 값데이터

    def __view_2depth_columns__(cols_1, cols_2):
        if DEBUG_MODE:
            print('\n\n')
            print(cols_1)
            print(cols_2)
            print(len(cols_1), len(cols_2))

    # Lv1 컬럼명
    cols_1 = list(df.columns)

    # Lv2 컬럼명
    _df = df.iloc[0:1, :]
    _df = _df.fillna('')
    cols_2 = _df.values[0]
    __view_2depth_columns__(cols_1, cols_2)

    # 순수 값 데이터
    _df2 = df.iloc[1:, :]
    # print(_df2.head())
    pure_data = list(_df2.values)

    ################################################################
    # 컬럼명 청소

    def __view_before_after_columns__(prev, next):
        if DEBUG_MODE:
            print('\n\n')
            print('전:', len(prev), prev)
            print('후:', len(next), next)

    # Unnamed 컬럼은 이전값으로 채운다
    def __fill_lv1_columns_(cols):
        prev = cols.copy()
        for i, c in enumerate(cols):
            if re.search('Unnamed', c) is not None:
                cols[i] = cols[i-1]

        __view_before_after_columns__(prev, cols)
        return cols
    
    def __clean_lv2_columns_(cols):
        prev = cols.copy()
        
        # 컬럼명 청소
        for i, c in enumerate(cols):
            if not isinstance(c, str):
                cols[i] = ''

        __view_before_after_columns__(prev, cols)
        return cols
    
    cols_1 = __fill_lv1_columns_(cols_1)
    cols_2 = __clean_lv2_columns_(cols_2)

    # return 

    ################################################################
    # 다중구조 컬럼명 프레임 생성

    colname_frame = []
    # 레벨2 인덱스
    _data = []
    for i, (c1, c2) in enumerate(zip(cols_1, cols_2)):
        # print([i, c1, c2])
        _data.append([c1, c2])
    df = pd.DataFrame(_data, columns=['c1', 'c2'])
    # print('\n\n')
    # print(df)

    def __add_to_colname_frame__(df):
        for d in df.to_dict('records'):
            cols = []
            for k,v in d.items():
                if len(v) > 0:
                    cols.append(v)
            colname_frame.append(cols)

    for n, g in df.groupby('c1', sort=False):
        # print('-'*100)
        # print([n, len(g)])
        if len(g) == 1:
            __add_to_colname_frame__(g)
        elif len(g) > 1:
            # data = g.to_dict('records')
            # print(data)
            g.c2 = g.c2.apply(lambda x: None if len(x) == 0 else x)
            g = g.ffill()
            # print(g)
            for n2, g2 in g.groupby(['c1','c2'], sort=False):
                k = 0 
                if len(g2) == 1:
                    __add_to_colname_frame__(g2)
                elif len(g2) > 1:
                    _data2 = g2.to_dict('records')
                    for d in _data2:
                        k+=1 
                        d.update({'c3': str(k)})
                    _g2 = pd.DataFrame(_data2)
                    # print(_g2)
                    __add_to_colname_frame__(_g2)

    # print('\n\n')
    # print(len(colname_frame))

    ################################################################
    # 데이터베이스 컬럼명으로 강제변환

    # print('\n\n')
    # new_cols = []
    # for li in colname_frame:
    #     # print(li)
    #     new_colname = "_".join(li)
    #     norm_colname = normalize_colname(new_colname)
    #     print([new_colname, norm_colname])
    #     new_cols.append(norm_colname)
    
    # df = pd.DataFrame(pure_data, columns=new_cols)
    return df


# [PARSER-TYPE-01] 1시트/1테이블/1depth_column 타입의 데이터를 구조화해서 CSV 파일로 저장 
def generate_csv_1depth_column(csv_file, df):

    # 데이터 청소
    df = clean_dataframe(df)

    # 컬럼명 정규화
    columns = get_normalized_colmap(list(df.columns))
    df = df.rename(columns=columns)

    # CSV 저장
    save_as_csv(csv_file, df)


# [PARSER-TYPE-02] 1시트/1테이블/2depth_column 타입의 데이터를 구조화해서 CSV 파일로 저장 
def generate_csv_2depth_column(csv_file, df):

    # 데이터 청소
    df = clean_dataframe(df)

    # 컬럼명 정규화
    df = normalize_2depth_columns(df)

    # CSV 저장
    save_as_csv(csv_file, df)

