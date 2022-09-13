import psycopg2
import pandas as pd
import xgboost as xgb


# ----------------------------------------GTU----------------------------------------------------------------------------------

def GTU(tr_1, new_df, vali, test):
    x = tr_1[["day", "hour", "minute", "dayofweek"]]  # train데이터 사용
    y = tr_1[["GTU"]]

    xvali = vali[["day", "hour", "minute", "dayofweek"]]  # validation 사용
    yvali = vali[["GTU"]]

    eval_set = [(xvali, yvali)]

    new_df["Date"] = test["index"]

    test = test[["day", "hour", "minute", "dayofweek"]]  # 예측데이터

    new_df['day'] = test['day']
    new_df['hour'] = test['hour']
    new_df['minute'] = test['minute']
    new_df['dayofweek'] = test['dayofweek']
    new_df.reset_index()

    xgb_model = xgb.XGBRegressor(n_estimators=250, learning_rate=0.07, subsample=0.7,
                                 colsample_bytree=0.7, max_depth=6, silent=1, nthread=4, min_child_weight=4)

    xgb_model.fit(x, y, eval_set=eval_set, eval_metric="rmse", early_stopping_rounds=15)
    pred = xgb_model.predict(test)

    new_df['GTU'] = pred

    return new_df


def ELPS_SUM(tr_1, new_df, vali):
    x = tr_1[["day", "hour", "minute", "dayofweek", "GTU"]]
    y = tr_1[["ELPS_AVG"]]

    xvali = vali[["day", "hour", "minute", "dayofweek", "GTU"]]  # validation 사용
    yvali = vali[["ELPS_AVG"]]

    eval_set = [(xvali, yvali)]

    test = new_df[["day", "hour", "minute", "dayofweek", "GTU"]]  # 예측데이터

    xgb_model = xgb.XGBRegressor(n_estimators=250, learning_rate=0.07, subsample=0.7,
                                 colsample_bytree=0.7, max_depth=6, silent=1, nthread=4, min_child_weight=4)

    xgb_model.fit(x, y, eval_set=eval_set, eval_metric="rmse", early_stopping_rounds=15)
    pred = xgb_model.predict(test)

    new_df['ELPS_AVG'] = pred

    return new_df


# -----------------------------------------------------ELPS_MSEC_MIN-------------------------------------------------------------------------

def ELPS_MIN(tr_1, new_df, vali):
    x = tr_1[["day", "hour", "minute", "dayofweek", "GTU"]]
    y = tr_1[["ELPS_MIN"]]

    xvali = vali[["day", "hour", "minute", "dayofweek", "GTU"]]  # validation 사용
    yvali = vali[["ELPS_MIN"]]

    eval_set = [(xvali, yvali)]

    test = new_df[["day", "hour", "minute", "dayofweek", "GTU"]]  # 예측데이터

    xgb_model = xgb.XGBRegressor(n_estimators=250, learning_rate=0.07, subsample=0.7,
                                 colsample_bytree=0.7, max_depth=6, silent=1, nthread=4, min_child_weight=4)

    xgb_model.fit(x, y, eval_set=eval_set, eval_metric="rmse", early_stopping_rounds=15)
    pred = xgb_model.predict(test)

    new_df['ELPS_MIN'] = pred

    return new_df


#-----------------------------------------------------ELPS_MSEC_MAX-------------------------------------------------------------------------

def ELPS_MAX(tr_1, new_df, vali):
    x = tr_1[["day", "hour", "minute","dayofweek", 'GTU']]
    y = tr_1[["ELPS_MAX"]]

    xvali = vali[["day", "hour", "minute", "dayofweek", "GTU"]]        #validation 사용
    yvali = vali[["ELPS_MAX"]]

    eval_set = [(xvali, yvali)]

    test = new_df[["day", "hour", "minute", "dayofweek", "GTU"]]            #예측데이터

    xgb_model = xgb.XGBRegressor(n_estimators=250, learning_rate=0.07, subsample=0.7,
                        colsample_bytree=0.7, max_depth=6, silent = 1, nthread = 4, min_child_weight = 4)


    xgb_model.fit(x, y, eval_set= eval_set, eval_metric="rmse", early_stopping_rounds=15)
    pred = xgb_model.predict(test)

    new_df['ELPS_MAX'] = pred

    return new_df




Db = "H2O"
User = "postgres"
PW = "<pwd>"
Host = "localhost"
Port = "5432"

table_name = "tps_predict"
original_table = "MINT_LOG"
TR_CODE = 'NRSTNCS12000'

with psycopg2.connect(dbname = Db, user = User, password=PW, host=Host, port=Port)as conn:
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {original_table}")
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=['TIME', 'TPS', 'ELPS_AVG', 'ELPS_MIN', 'ELPS_MAX'])
        # df = df.loc[df['TRCODE'] == TR_CODE]

        df["GTU"] = df.TPS / 10

        df['Date'] = df.TIME.apply(lambda x: str(x))
        df['year'] = df.TIME.apply(lambda x: str(x)[:4])
        df['month'] = df.TIME.apply(lambda x: str(x)[4:6])
        df['day'] = df.TIME.apply(lambda x: str(x)[6:8])
        df['hour'] = df.TIME.apply(lambda x: str(x)[8:10])
        df['minute'] = df.TIME.apply(lambda x: str(x)[10:12])

        df.Date = pd.to_datetime(df.Date, format="%Y%m%d%H%M%S")

        df["dayofweek"] = df.Date.dt.dayofweek
        # --------------------------------------------------------------TPS--------------------------------------------------------------------------------

        df.day = df.day.astype('int')
        df.hour = df.hour.astype('int')
        df.minute = df.minute.astype('int')
        new_df = pd.DataFrame()


        #학습에는 1달전 ~ 2주전까지 데이터만 사용
        # tr_1 = df[(df['TRCODE'] == TR_CODE)]
        tr_1 = df[:-288]
        tr_1 = tr_1[
            ['TPS', 'GTU', 'ELPS_AVG', 'ELPS_MIN', 'ELPS_MAX', 'Date', 'year', 'month', 'day', 'hour',
             'minute', 'dayofweek']]

        #마지막 1주일 데이터로 validation
        # comp = df[(df['TRCODE'] == TR_CODE)]
        comp = df[-288:]
        comp = comp[
            ['TPS', 'GTU', 'ELPS_AVG', 'ELPS_MIN', 'ELPS_MAX', 'Date', 'year', 'month', 'day', 'hour',
             'minute', 'dayofweek']]
        #-----------------------------------------------test-----------------------------------------------------------
        #다음 1주일치 예측을 위한 df생성
        year = "2022"
        month = "08"
        start_date = "07"
        testdataset = pd.date_range(f"{year}/{month}/{start_date} 19:10", f"{year}/{month}/{'0' + str(int(start_date) + 2)} 19:10", freq="10T")

        testdata = pd.DataFrame(range(len(testdataset)), index=testdataset)

        testdata["year"] = testdata.index.year  # 연도 정보
        testdata["month"] = testdata.index.month  # 월 정보
        testdata["day"] = testdata.index.day  # 일 정보
        testdata["hour"] = testdata.index.hour  # 시간 정보
        testdata["minute"] = testdata.index.minute  # 분 정보
        testdata["dayofweek"] = testdata.index.dayofweek

        testdata = testdata.reset_index()
        testdata = testdata.drop(0, axis=1)
       # ----------------------------------------main----------------------------------------------------------------------------------
        new_df = GTU(tr_1, new_df, comp, testdata)
        new_df = ELPS_SUM(tr_1, new_df, comp)
        new_df = ELPS_MIN(tr_1, new_df, comp)
        new_df = ELPS_MAX(tr_1, new_df, comp)

        x = tr_1[["day", "hour", "minute", "dayofweek", 'GTU', "ELPS_AVG", "ELPS_MIN", "ELPS_MAX"]]
        y = tr_1[["TPS"]]

        xvali = comp[["day", "hour", "minute", "dayofweek", 'GTU', "ELPS_AVG", "ELPS_MIN", "ELPS_MAX"]]
        yvali = comp[["TPS"]]

        eval_set = [(xvali, yvali)]

        test = new_df[["day", "hour", "minute", "dayofweek", "GTU", "ELPS_AVG", "ELPS_MIN", "ELPS_MAX"]]

        xgb_model = xgb.XGBRegressor(n_estimators=950, learning_rate=0.07, subsample=0.7,
                                     colsample_bytree=0.7, max_depth=6, silent=1, nthread=4, min_child_weight=4)

        xgb_model.fit(x, y, eval_set=eval_set, eval_metric="rmse", early_stopping_rounds=15)

        pred = xgb_model.predict(test)

        new_df["TPS"] = pred

        new_df.Date = new_df.Date.astype('str')
        new_df['TIME'] = new_df.Date.apply(lambda x: x[:4] + x[5:7] + x[8:10] + x[11:13] + x[14:16]+"00")

        new_df = new_df[["TIME", "TPS", "ELPS_AVG", "ELPS_MIN", "ELPS_MAX"]]

        # 파일로 저장
        new_df.to_csv(
            '/TPS_bound.csv',
            sep=",",
            na_rep="NaN",
            index=False,
            header=False)

        # 파일을 기존 테이블 제거후 업데이트
        create_query = f"""
                        DROP TABLE IF EXISTS {table_name};
                        CREATE TABLE {table_name}(
                        TIME BIGINT,
                        TPS FLOAT8,
                        ELPS_AVG FLOAT8,
                        ELPS_MIN FLOAT8,
                        ELPS_MAX FLOAT8)"""
        # TRCODE VARCHAR(30),
        cur.execute(create_query)
        input_query = f"COPY {table_name} FROM STDIN DELIMITER ',' CSV;"
        with open('/TPS_bound.csv', 'r') as f:
            cur.copy_expert(input_query, f)
        print("executed")
        conn.commit()
