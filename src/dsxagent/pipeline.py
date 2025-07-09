# -*- coding: utf-8 -*-
import json 
import os 

import pandas as pd


import fileio


def generate_summary_csv(read_dir, write_dir):
    jsons = fileio.ifile.get_files(read_dir, type='json', fullpath=True)
    data = []
    print('\n\n')
    for file in sorted(jsons):
        print('PATH:', file)
        js = fileio.ifile.read_jsonfile(file)
        # print('JSON:', js)
        pipe_name = js['pipeline']['name']
        # print('PIPELINE:', pipe_name)
        path = js['pipeline']['config']['dataflow']['filter']['prefix']
        # print('STORAGE_PATH:', path)
        # break
        data.append({'pipeline_name': pipe_name, 'source_file': path})
    
    df = pd.DataFrame(data)
    csv_file = os.path.join(write_dir, "DataPipelineRelations.csv")
    fileio.save_as_csv(csv_file, df)


def convert_into_tis_json_format(text, title, srcfile, **kwargs):
    item = {
        'srcfile': srcfile,
        'title': title,
        'text': text
    }
    item.update(kwargs) 
    return [{
        'action': 'AddOrReplaceItem',
        'item': item
    }]



################################################################
# 다쏘시스템 Json Event Data Format 
################################################################

class JsonEventData:

    # 샘플데이터 
    {
        "action": "AddOrReplaceItem",
        "item": {
            "uri": "abcd123",
            "text": "this is an example to analyze",
            "title": "My Test",
            "docUtcDate": "1111-01-11"
        }
    }

    def __init__(self):
        pass

    def transform(self, data):
        output = []
        for d in data:
            output.append({
                "action": "AddOrReplaceItem",
                "item": d
            })
        return output 