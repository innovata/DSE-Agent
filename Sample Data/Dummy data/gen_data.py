

import os, sys 
import json 

TOP_DIR = os.path.dirname(__file__)


class_path = "pkg.TestClass-01"

data = []
for i in range(10):
    data.append({
        'class': class_path,
        'title': f'Test title-{str(i).zfill(2)}',
        '_text': f"This is a test text for ingestion-{str(i).zfill(2)}"
    })


with open(r"F:\pypjts\DSE-Agent\Sample Data\Dummy data\SGI_Input_data.json", "w", encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)