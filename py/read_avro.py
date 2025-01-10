import fastavro
import pandas as pd
import json
import codecs
def read_avro_file(file_path):
    with open(file_path, 'rb') as f:
        reader = fastavro.reader(f)
        records = []
        for record in reader:
            records.append(record)
        return records

# 读取Avro文件
avro_file_path = '/home/fishsun/IdeaProjects/flink-paimon-it/lakehouse/layout.db/layout1/manifest/manifest-e756f5a0-2420-4932-b802-05328c4f3e77-0'

avro_data = read_avro_file(avro_file_path)

print(avro_data)

# print(json.dumps(avro_data))
