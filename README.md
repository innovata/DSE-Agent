# NETVIBES / Data Science Experience Agent


<!-- ############################################################ -->
## 1. INTRODUCTION  
<!-- ############################################################ -->

DSE-Agent 패키지는 클라이언트 사이드에서 클라우드 솔루션인 DSE 사용을 지원하는 몇가지 기능을 제공합니다.
'Data Science Experience (DSE)' 는 NETVIBES 브랜드의 솔루션 포르폴리오 중 하나인 솔루션명 입니다.  


<!-- ############################################################ -->
## 2. 환경구성 
<!-- ############################################################ -->

### [1] 파이썬 가상환경 구성 (Setup Python Virtual Environment) 

다음 링크의 가이드를 참조하세요.  

https://github.com/innovata/DevDocs/blob/main/PythonEnv.md  


### [2] dsx-agent 패키지 설치 

파이썬 가상환경에서, 다음 커멘드를 실행하여 패키지 설치.

    pip install dse-agent

    (pip install -r requirements.txt)


<!-- ############################################################ -->
## 3. 커멘드라인 사용 예제 
<!-- ############################################################ -->

### [1] 커멘드라인 기본 구조 

        python dse-agent.py [options]

Options:  
--func="upload_dir"   
--resourceUUID="YOUR_TARGET_OBJECT_STORAGE_ID"  
--top_dir="D:\__3DDrive__"  
... ETC 


### [2] 스토리지 검색 

        python dse-agent.py --func="search_storage" --name="TestDATASET-01"

### [3] 파일 업로드 

top_dir 이하 모든 파일들을 한번에 업로드

        python dse-agent.py --func="upload_dir" --resourceUUID="YOUR_TARGET_OBJECT_STORAGE_ID" --top_dir="D:\__3DDrive__"
        





<!-- ############################################################ -->
## 4-1. 인라인 사용 예제 | REST API 
<!-- ############################################################ -->


### [1.1] dsxagent.restapi.Storages 

1.1.1. 모든 스토리지 가져오기 & DB저장

        from dsxagent import restapi 

        storage = restapi.Storages()
        storage.get_n_save()

        <!-- 데이터베이스 확인 -->


1.1.2.  스토리지 검색 

        from dsxagent import restapi 
        storage = restapi.Storages()
        doc = storage.search_by_name(name="TestDATASET-01", workspace_id="dw-global-000000-default")
        cards = doc["json_response"]["cards"]
        if len(cards) == 1: 
                cards[0]
        else:
                print("검색 범위를 좁히세요. 또는 추가로 코드를 작성하세요.")

cards[0] 샘플 데이터 -->

        {
                "id": "0a28ee53-4def-4c92-ba75-87537d83185f	",
                "resourceId": "TestDATASET-01",
                "projectId": "dp-global-000000",
                "resourceUUID": "0a28ee53-4def-4c92-ba75-87537d83185f	",
                "workspaceId": "dw-global-000000-default",
                "name": "TestDATASET-01",
                "creator": "jle69_gmail",
                "kind": "Storage",
                "type": "ObjectStorage",
                "created": 1751954756480,
                "lastModified": 1751954756485,
                "permissions": {
                        "read": true,
                        "write": true,
                        "execute": true
                }
        }


1.1.3.  스토리지 삭제 


### [1.2] dsxagent.restapi.ObjectStorage 

1.2.1. 파일 (벌크) 업로드 

앞서 설명한 1.1.2. 에서 resourceUUID 를 사용.

        from dsxagent import restapi 
        obj = restapi.ObjectStorage(resourceUUID="0a28ee53-4def-4c92-ba75-87537d83185f")
        obj.upload_many(files=[file1, file2, ... fileN]) # file1: 파일절대경로

        <!-- 이후 커멘트 상태확인 -->


### [1.3] dsxagent.restapi.SemanticGraphIndex 



### [1.4] dsxagent.restapi.Pipeline 



<!-- ############################################################ -->
## 4-2. 인라인 사용 예제 | Data Models  
<!-- ############################################################ -->


 
