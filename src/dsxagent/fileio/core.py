# -*- coding: utf-8 -*-



import os  
import re 
import pprint 
pp = pprint.PrettyPrinter(indent=2)
import json 
from pathlib import WindowsPath

import pandas as pd

from ipylib import ifile




################################################################
# FILE PATH 
################################################################

import math 



def is_selective_docs(file, doctypes):
    # doctypes = ['pptx','pdf','txt','xlsx','docx','xlsm']
    doctypes = ['.'+elem for elem in doctypes]
    conds = [file.endswith(_type) for _type in doctypes]
    # print(conds)
    # OR 조건
    return math.fsum(conds)
    # AND 조건
    return math.prod(conds)


def get_target_files(
        top_dir:str, 
        doctypes:list=['pptx','pdf','txt','xlsx','docx','xlsm'],
        show_on_cmd:bool=False
):

    targets = []
    for root, dirs, files in os.walk(top_dir, topdown=True):
        for file in files:
            if is_selective_docs(file, doctypes):
                _f = os.path.join(root, file)
                targets.append(_f)
                if show_on_cmd:
                    print(f"'{_f}'")
    
    return targets




################################################################
# FILE I/O 
################################################################

def check_english_filename(filepath):
    filename = os.path.basename(filepath)
    # root, ext = os.path.splitext(filename)
    if check_has_korean(filename):
        print(f'ERROR | 경로를 제외한 파일명에 한글이 있으면 저장하지 않는다! 입력된 파일명: {filename}')
        return False 
    else:
        return True 


# 문자열에 한글이 있는지 없는지 체크
def check_has_korean(s):
    return True if re.search('[가-힣]+', s) is not None else False


def check_columns_are_english(cols):
    li = [c for c in cols if check_has_korean(c)]
    dic = {}
    for c in li:
        dic.update({c: ''})
    print('아래 Dictionary를 이용하여 컬럼들을 영어로 변경하라')
    pp.pprint(dic)
    return True if len(li) == 0 else False


def save_as_csv(csv_file, df):
    csv_file = str(WindowsPath(csv_file))
    # 경로를 제외한 파일명에 한글이 있으면 저장하지 않는다
    filename = os.path.basename(csv_file)
    if re.search('[가-힣]+', filename) is None:
        passfail = df.to_csv(csv_file, index=False, encoding='utf-8')
        print('Pass/Fail:', passfail)
        print('Success:', csv_file)
    else:
        print(f'ERROR | 경로를 제외한 파일명에 한글이 있으면 저장하지 않는다! 입력된 파일명: {filename}')


def write_csv(csv_file, df):
    csv_file = str(WindowsPath(csv_file))
    try:
        df.to_csv(csv_file, index=False, encoding='utf-8')
    except Exception as e:
        print(f"ERROR | {e}")
    else:
        print('Success:', csv_file)


def read_csv(csv_file, dtype=None):
    return ifile.read_csvfile()


def load_csv(topdir, filename):
    csv_file = ifile.find_file(topdir, filename)
    if os.path.exists(csv_file):
        return pd.read_csv(csv_file)
    else:
        print(f'ERROR | CSV파일의 경로를 확인하시오: {csv_file}')
            

class FilenameCleaner():

    def __init__(self):
        pass 

    # Data Factory Studio 에서 데이터 저장시, 경로 포함 파일명은 전부 English 이어야 한다
    def _clean_filename(self, filename):
        if re.search('[가-힣]+', filename) is None:
            return filename 
        else: 
            print('Data Factory Studio 에서 데이터 저장시, 경로 포함 파일명은 전부 English 이어야 한다! 원본파일명:', filename)

            # CASE.01
            if re.search('연계', filename) is not None:
                filename = re.sub('연계', repl='_related_', string=filename)
                return filename
            else:
                return None 


def normalize_columns(columns):
    colsmap = {}
    for col in columns:
        c = re.sub('_x000D_', repl='', string=col)
        c = c.lower()
        c = re.sub('[\s-]+', repl='_', string=c)
        c = re.sub("[\.']+", repl='', string=c)
        c = re.sub("^class$", repl='class_', string=c)
        c = re.sub("^matl$", repl='material', string=c)
        c = re.sub("%", repl='pct', string=c)
        c = re.sub("/", repl='_n_', string=c)
        colsmap.update({col: c})

    return colsmap


def __normalize_2depth_columns(df):
    cols_1 = list(df.columns)
    _df = df.iloc[0:1, :]
    _df = _df.fillna('')
    cols_2 = _df.values[0]
    # print(cols_1)
    # print(cols_2)
    # print(len(cols_1), len(cols_2))

    # Unnamed 컬럼은 이전값으로 채운다
    old_cols = cols_1.copy()
    for i, c in enumerate(cols_1):
        # print(i, c)
        if re.search('Unnamed', c) is not None:
            cols_1[i] = cols_1[i-1]
    # print(cols_1)
    
    new_cols = []
    for c1, c2 in zip(cols_1, cols_2):
        # print(c1, c2)
        new_cols.append(f"{c1} {c2}")
        # if len(c2) == 0:
        #     columns.update({c1: c1})
        # else:
    # print(new_cols)

    columns = {}
    for c1, c2 in zip(old_cols, new_cols):
        columns.update({c1: c2.strip()})

    df = df.rename(columns=columns)
    columns = normalize_columns(list(df.columns))
    df = df.rename(columns=columns)
    df = df.iloc[1:, :]
    return df



# 파일명으로 폴더 생성 
def create_dir_by_filename(xslx_file):
    path = os.path.dirname(xslx_file)
    filename = os.path.basename(xslx_file)
    root, ext = os.path.splitext(filename)
    new_dir = os.path.join(path, root)
    try:
        os.mkdir(new_dir)
    except OSError as e:
        print(f'WARNING | {e}')
    else:
        print(f'파일명 폴더 생성 성공: {new_dir}')
    finally:
        return new_dir


# 엑셀파일명을 DFS 패키지명으로 변환
def normalize_excelname_to_packagename(xslx_files):
    for filepath in xslx_files:
        file = os.path.basename(filepath)
        filename, ext = os.path.splitext(file)
        pkg_name = normalize_pkgname(filename)
        print(f"{file} --> {pkg_name}")
    return 


# 엑셀파일명을 DFS 패키지명으로 변환
def generate_dfs_storage_path(project_name, filepath):
    # 파일타입 
    root, ext = os.path.splitext(filepath)
    filetype = ext.replace('.', '').strip().upper()

    if filetype in ['CSV', 'JSON']:
        datatype = 'DemoData'
        filename = os.path.basename(os.path.dirname(filepath))
    elif filetype in ['XLSX', 'XLS']:
        datatype = 'RawData'
        filename, ext = os.path.splitext(os.path.basename(filepath))

    return f"{project_name}/{datatype}/{filetype}/{filename}/"


def write_json(obj, filepath):
    filepath = str(WindowsPath(filepath))
    print("filepath:", filepath)
    new_dir = os.path.dirname(filepath)
    print("new_dir:", new_dir)
    try:
        os.mkdir(new_dir)
    except OSError as e:
        print(f'WARNING | {e}')
    else:
        print(f'파일명 폴더 생성 성공: {new_dir}')

        with open(filepath, 'w', encoding='utf-8') as fp:
            json.dump(obj, fp, ensure_ascii=False)
            fp.close()


def read_json(filepath):
    filepath = str(WindowsPath(filepath))
    with open(filepath, 'r', encoding='utf-8') as fp:
        js = json.load(fp)
        fp.close()
    return js 


 
