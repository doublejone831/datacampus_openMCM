import math

from kafka import KafkaConsumer
from json import loads
import psycopg2
import pandas as pd


Db = "H2O"
User = "postgres"
PW = "<pwd>"
Host = "localhost"
Port = "5432"
tps_limit = "TPS_predict"
elps_limit = "limits"

consumer = KafkaConsumer(
    'TIME_LOG',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
)

#------------------------------------------------------------------------------

def check_TPS(log,bound):
    if log["TPS"] < bound["TPS_LOWER"] or log["TPS"] > bound["TPS_UPPER"]:
        notice_error('tps', log)
        return -1
    return 0


def check_ELPS(log, bound):
    if log["ELPS_MAX"] < bound["ELPS_MAX_LOWER"] or log["ELPS_MAX"] > bound["ELPS_MAX_UPPER"]:
        notice_error('ELPS_MAX', log)
        return -1
    elif log["ELPS_AVG"] < bound["ELPS_AVG_LOWER"] or log["ELPS_AVG"] > bound["ELPS_AVG_UPPER"]:
        notice_error('ELPS_AVG', log)
        return -1
    else:
        return 0


def notice_error(e, log):
    print(f"!!WARNING!!  {log['TIME']} : {e} is different than predicted.")
    print("--------------------------------------------------")


#------------------------------------------------------------------------------

with psycopg2.connect(dbname=Db, user=User, password=PW, host=Host, port=Port) as conn:
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {tps_limit}")
        rows = cur.fetchall()
        TPS_predict = pd.DataFrame(rows, columns=['TIME', 'TPS', 'ELPS_AVG', 'ELPS_MIN', 'ELPS_MAX'])
        print(TPS_predict)
        cur.execute(f"SELECT * FROM {elps_limit}")
        rows = cur.fetchall()
        ELPS_predict = pd.DataFrame(rows, columns=['TPS', 'ELPS_AVG_UPPER', 'ELPS_AVG_LOWER', 'ELPS_MAX_UPPER', 'ELPS_MAX_LOWER'])
        print(ELPS_predict)
if not consumer.bootstrap_connected():
    print("consumer not connected to broker")
else:
    print("consumer connected")
    while True:
        for message in consumer:
            log = message.value
            ELPS_bound = ELPS_predict[ELPS_predict["TPS"] == (log["TPS"]//100)*100]
            TPS_bound = TPS_predict[TPS_predict['TIME'] == log["TIME"]]
            print(ELPS_bound)
            print(TPS_bound)
            bound = {"ELPS_AVG_UPPER": ELPS_bound['ELPS_AVG_UPPER'].values,
                     "ELPS_AVG_LOWER": ELPS_bound['ELPS_AVG_LOWER'].values,
                     "ELPS_MAX_UPPER": ELPS_bound['ELPS_MAX_UPPER'].values,
                     "ELPS_MAX_LOWER": ELPS_bound['ELPS_MAX_LOWER'].values,
                     "TPS_UPPER": TPS_bound['TPS'].values*1.33,
                     "TPS_LOWER": TPS_bound['TPS'].values*0.66
                     }
            print(bound)
            print(f"current status {log}")
            if check_TPS(log, bound) != 0 and check_ELPS(log, bound) != 0:
                print(f"!!WARNING!! TPS and ELSP out of range: Check the system")
            else:
                print("correct")

