import psycopg2
from json import loads
import pandas.io.sql as psql
import pandas as pd
import numpy as np
import scipy.stats as st


Db = "H2O"
User = "postgres"
PW = "<pwd>"
Host = "localhost"
Port = "5432"

table_name = "limits"
original_table = "MINT_LOG"
TR_CODE ='NRSTNCS12000'
gap = 10
#위아래 경계치 계산
def get_bound(df,column):
    upper = df[column].quantile(q=0.995, interpolation='nearest')
    lower = df[column].quantile(q=0.005, interpolation='nearest')
    return upper, lower

#경계치를 찾아서 리스트로 TPS에 따른 딕셔너리로 반환
def find_bound(df, column, iteration):
    lis = {}
    for j in iteration:
        data = df.loc[df['TPS'] < j + gap]
        data = data.loc[df['TPS'] > j]
        upper, lower = get_bound(data, column)
        if lower == None or upper == None:
            if j != 1:
                lis[j] = lis[j-1]
            continue
        else:
            lis[j] = [upper, lower]
    return lis


with psycopg2.connect(dbname = Db, user = User, password=PW, host=Host, port=Port)as conn:
    with conn.cursor() as cur:
        #전체 DB불러와서 DF로 변환
        cur.execute(f"SELECT * FROM {original_table}")
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=['TIME','TPS','ELPS_AVG','ELPS_MIN','ELPS_MAX'])
        #원하는 특정 TRCODE 가져오기
        # df_ = df.loc[df['TRCODE'] == TR_CODE]

        iteration = np.arange(0, df['TPS'].values.max(), gap)

        #경계값 찾기
        AVG = find_bound(df, "ELPS_AVG", iteration)
        MAX = find_bound(df, "ELPS_MAX", iteration)


        #경계값을 df로 저장
        li = []
        for i in iteration:
            li.append([i, AVG[i][0], AVG[i][1], MAX[i][0], MAX[i][0]])
        final_df = pd.DataFrame(li,
                                columns=['TPS', 'ELPS_AVG_UPPER', 'ELPS_AVG_LOWER', 'ELPS_MAX_UPPER', 'ELPS_MAX_LOWER'])
        final_df.fillna(method = "ffill")
        #파일로 저장
        final_df.to_csv(
            '/TPS_bound.csv',
            sep = ",",
            na_rep = "NaN",
            index = False,
            header = False)

        #파일을 기존 테이블 제거후 업데이트
        create_query = f"""
                DROP TABLE IF EXISTS {table_name};
                CREATE TABLE {table_name}(
                TPS FLOAT8,
                ELPS_AVG_UPPER FLOAT8,
                ELPS_AVG_LOWER FLOAT8,
                ELPS_MAX_UPPER FLOAT8,
                ELPS_MAX_LOWER FLOAT8)"""
        cur.execute(create_query)
        input_query=f"COPY {table_name} FROM STDIN DELIMITER ',' CSV;"
        with open('/TPS_bound.csv', 'r') as f:
            cur.copy_expert(input_query, f)
        print("executed")
        conn.commit()
