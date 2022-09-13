import psycopg2
from kafka import KafkaConsumer
from json import loads

#DB접속용
Db = "H2O"
User = "postgres"
PW = "<pwd>"
Host = "localhost"
Port = "5432"

#카프카 컨슈머 생성
consumer = KafkaConsumer(
    'TIME_LOG',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')),
)
#연결 확인
if not consumer.bootstrap_connected():
    print("consumer not connected to broker")
else:
    print("consumer connected")
#불러올 테이블 이름
table_name = "MINT_LOG"

#PostgreSQL 연결
with psycopg2.connect(dbname = Db, user = User, password=PW, host=Host, port=Port)as conn:
    with conn.cursor() as cur:
        #기존에 테이블이 존재 하지않으면 생성
        create_query = f"""CREATE TABLE IF NOT EXISTS {table_name}(
        TR_YMD_HMS BIGINT,
        TPS FLOAT8,
        ELPS_AVG FLOAT8,
        ELPS_MIN FLOAT8,
        ELPS_MAX FLOAT8)"""
        # TRCODE VARCHAR(30),
        cur.execute(create_query)
        conn.commit()
        #실시간으로 들어오는 데이터를 DB에 저장
        while True:
            for message in consumer:
                data = message.value
                cur.execute(f"INSERT INTO {table_name} "
                            f"VALUES {data['TIME'],data['TPS'], data['ELPS_AVG'], data['ELPS_MIN'], data['ELPS_MAX']}")
                print("executed")
                conn.commit()
