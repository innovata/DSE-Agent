# -*- coding: utf-8 -*-


import zipfile
import xml.etree.ElementTree as ET
import os
import pprint 
pp = pprint.PrettyPrinter(indent=2)



def read_3dxml_file(file_path)-> dict:
    """
    3DXML 파일을 읽어 내부 XML 데이터를 파싱합니다.
    Args:
        file_path (str): 3DXML 파일 경로
    Returns:
        dict: 파싱된 XML 데이터 또는 None
    """
    try:
        print("\n3D XML-->", file_path)

        # 3DXML 파일을 ZIP 아카이브로 열기
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # ZIP 내부 파일 목록 확인
            file_list = zip_ref.namelist()
            print("\nZIP 아카이브 내 파일 목록-->")
            pp.pprint(file_list)

            # 임시 디렉토리에 ZIP 내용 추출
            temp_dir = "temp_3dxml_extract"
            os.makedirs(temp_dir, exist_ok=True)
            zip_ref.extractall(temp_dir)

            # XML 파일(예: Manifest.xml 또는 *.xml) 찾아 파싱
            _len = len(file_list)
            for i, file_name in enumerate(file_list, start=1):
                print(f"\n{'-'*50} ({i}/{_len}) {file_name}")

                if file_name.endswith('.xml'):
                    xml_file_path = os.path.join(temp_dir, file_name)
                    try:
                        # XML 파일 파싱
                        tree = ET.parse(xml_file_path)
                        root = tree.getroot()

                        # XML 데이터 탐색 (예: 루트 태그와 첫 번째 하위 요소 출력)
                        print(f"XML 파일: {file_name}")
                        print(f"루트 태그: {root.tag}")
                        for child in root:
                            print("\nchild-->", [child, type(child)])
                            print(f"하위 요소: {child.tag}, 속성: {child.attrib}")
                            print(dir(child))

                        # 필요 시 XML 데이터를 딕셔너리로 변환
                        xml_data = {
                            'root_tag': root.tag,
                            'root_attrib': root.attrib,
                            'children': [
                                {'tag': child.tag, 'attrib': child.attrib, 'text': child.text}
                                for child in root
                            ]
                        }
                        return xml_data

                    except ET.ParseError as e:
                        print(f"\nXML 파싱 에러 ({file_name}): {e}")
                    except Exception as e:
                        print(f"\n파일 처리 중 에러 ({file_name}): {e}")

            print("\nXML 파일을 찾을 수 없습니다.")
            return None

    except zipfile.BadZipFile:
        print("\n에러: 유효하지 않은 3DXML 파일입니다. ZIP 형식이 맞는지 확인하세요.")
        return None
    except FileNotFoundError:
        print(f"\n에러: 파일 {file_path}을(를) 찾을 수 없습니다.")
        return None
    except Exception as e:
        print(f"\n에러: {e}")
        return None


def extract_3dxml(file_path):
    """
    3DXML 파일의 압축을 풀어 내부 파일을 추출합니다.
    Args:
        file_path (str): 3DXML 파일 경로
        extract_dir (str): 추출할 디렉토리 경로 (기본값: extracted_3dxml)
    Returns:
        bool: 성공 시 True, 실패 시 False
    """
    try:
        # 출력 디렉토리 생성
        extract_dir = os.path.splitext(file_path)[0]
        os.makedirs(extract_dir, exist_ok=True)

        # 3DXML 파일을 ZIP 아카이브로 열기
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # ZIP 내부 파일 목록 출력
            file_list = zip_ref.namelist()
            # print("ZIP 아카이브 내 파일 목록:", file_list)

            # 모든 파일 추출
            zip_ref.extractall(extract_dir)
            print(f"\n파일이 디렉토리({{extract_dir}})에 성공적으로 추출되었습니다.")
            return True

    except zipfile.BadZipFile:
        print("에러: 유효하지 않은 3DXML 파일입니다. ZIP 형식이 맞는지 확인하세요.")
        return False
    except FileNotFoundError:
        print(f"에러: 파일 {file_path}을(를) 찾을 수 없습니다.")
        return False
    except PermissionError:
        print(f"에러: {extract_dir} 디렉토리에 쓰기 권한이 없습니다.")
        return False
    except Exception as e:
        print(f"에러: {e}")
        return False

import re 

def read_BriefcaseList_xml(xml_file):
    _dir = os.path.splitext(xml_file)[0]
    target_file = os.path.join(_dir, "BriefcaseList.xml")
    with open(target_file, 'r') as f:
        text = f.read()
        f.close() 
    
    tags = re.findall(r'<[a-zA-Z0-9\s"=\-_]+/>', text)
    new_tags = []
    for tag in tags:
        # print("\n-->", tag)
        attrs = re.findall(r'([A-Za-z\s]+)\s*=\s*"([a-zA-Z0-9\s_\-]*)"', tag)
        new_tag = {}
        for k,v in attrs:
            new_tag.update({k.strip(): v.strip()})
        # pp.pprint(new_tag)
        new_tags.append(new_tag)
    
    print("\n태그 개수-->", len(new_tags))
    return new_tags



def get_PLM_ExternalIDs(xml_file):

    data = read_BriefcaseList_xml(xml_file)

    ids = []
    for d in data:
        if re.search(r"^prd-", d['Value']):
            ids.append(d['Value'])

    print("\nPLM_ExternalID 개수-->", len(ids))
    return sorted(ids) 
            


