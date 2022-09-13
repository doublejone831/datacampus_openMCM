import sys
import numpy as np
import pandas as pd
import torch
import base64
from kafka import KafkaProducer
import json
import time
from kafka.errors import KafkaError
import logging
# from tqdm import tqdm

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(excp):
    logging.error('I am an errback', exc_info=excp)


#tensor->numpy->base64 representation for REST post
def Encode_tensor_to_bytes(input: pd.DataFrame) -> str:
    numpy_arr = input.to_numpy()
    bytes_numpy = base64.b64encode(numpy_arr).decode()
    return bytes_numpy


Path = 'shooting_log.csv'

df = pd.read_csv(Path, names = ['TIME', 'TPS', 'ELPS_AVG', 'ELPS_MIN', 'ELPS_MAX'])

producer = KafkaProducer(bootstrap_servers='localhost:9092')
if not producer.bootstrap_connected():
    print("producer not connected to broker")
else:
    print("producer connected to broker")
    # image, label = train_dataset[0]
    for i in range(len(df)):
        data = {"TIME": int(df.loc[i]['TIME']),
                "TPS":  float(df.loc[i]['TPS']),
                "ELPS_AVG":  float(df.loc[i]['ELPS_AVG']),
                "ELPS_MIN":  float(df.loc[i]['ELPS_MIN']),
                "ELPS_MAX":  float(df.loc[i]['ELPS_MAX'])}
        print(data)
        sending_data = json.dumps(data).encode('utf-8')
        producer.send(topic='TIME_LOG', value=sending_data).add_callback(on_send_success).add_errback(on_send_error)
        producer.flush()
        time.sleep(5)


