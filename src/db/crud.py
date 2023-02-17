from setting import ENGINE, session
import pandas as pd
import numpy as np
from datetime import datetime
from models import Charge_Sat
from sqlalchemy import func
from typing import Tuple



def charge_count(channel_array):
    count = 0
    for ch in channel_array:
        if ch > 0:
            count += 1
    return count

# 1つのcsvをデータベースに挿入
def InsertChargeData(path : str, sta_index : int) -> None:
    # ローカルのデータを読み込み
    df = pd.read_csv(path, parse_dates=['date'])
    df.set_index('date', inplace=True)
    # 1分ごとに集計
    charge_count_array = df.charge_channel.resample('MIN').apply(charge_count)
    mag_lat_array = df.mag_lat.resample('MIN').first().values
    mag_ltime_array = df.mag_lat.resample('MIN').first().values
    date = charge_count_array.index.to_pydatetime()
    charge_count_array = charge_count_array.values
    sat_id = [sta_index] * len(date)
    # データフレーム化
    columns = ['satellite_id', 'date', 'lat', 'lon', 'charge_count']
    output_df = pd.DataFrame(np.array([sat_id, date, mag_lat_array, mag_ltime_array, charge_count_array]).T, columns=columns)
    output_df["created_at"] = datetime.now()
    # データベースへ書き込み
    output_df.to_sql("charge",con=ENGINE, if_exists="append", method="multi", index=False)


# 一定期間のファイルをデータベースに挿入
def InsertAll(sat_index : int, start_year : int, end_year : int) -> None:
    for year in range(start_year, end_year+1):
        for month in range(1, 13):
            for day in range(1, 32):

                month_str = str(month).zfill(2)
                day_str = str(day).zfill(2)
                path = f'/Volumes/USB/Processed_Data/dmsp-f{sat_index}/{year}/{month_str}/dmsp-f{sat_index}_{year}{month_str}{day_str}.csv'

                # データベースに挿入
                try :
                    InsertChargeData(path=path, sta_index=sat_index)
                    print(year, month, day)
                except:
                    continue
            
# 帯電しているデータを取得
def ReadChargeDate():
    # SELECT
    sat_data = session.query(Charge_Sat).filter(Charge_Sat.charge_count > 0)
    tmp = []
    for sat in sat_data:
        tmp.append([sat.date, sat.lat, sat.lon, sat.charge_count])
    df = pd.DataFrame(tmp, columns=['date', 'lat', 'lon', 'charge_count'])
    df.to_csv('charge.csv', index=False)

# 時刻に対応するidを取得
def get_date_id(satellite_id, YMD : datetime) -> int:
    response = session.query(Charge_Sat.id).filter(
        Charge_Sat.satellite_id == satellite_id,
        Charge_Sat.date == YMD
        ).first()
    return response[0]


# 1分後に帯電しているレコードを取得
def get_charge_next(start_id: int, end_id : int) -> Tuple[list, int]:
    # サブクエリー
    subquery = (session.query(
        Charge_Sat.satellite_id,
        Charge_Sat.date,
        Charge_Sat.lat,
        Charge_Sat.lon,
        Charge_Sat.charge_count,
        func.lead(Charge_Sat.charge_count, 1).over(order_by=Charge_Sat.date).label('next_count'),
    ).filter(
        Charge_Sat.id.between(start_id, end_id)
        )).subquery('sub')
    
    # メインクエリー
    response = session.query(subquery).filter(subquery.c.next_count > 0).all()

    output = []
    for satellite_id, date, lat, lon, charge_count, next_count in response:
        output.append([satellite_id, date, lat, lon, charge_count, next_count])
    
    next_ind = end_id + 1
    return output, next_ind

# 全て取得
def GetChargeDataAll():
    start_id = 1
    last_id = session.query(Charge_Sat.id).order_by(
        Charge_Sat.id.desc()
        ).first()[0]
    
    output = []
    while start_id < last_id:
        # 取得期間
        end_id = start_id + 100000 # 1度に100,000レコードしか取得できないため
        if end_id >= last_id:
            end_id = last_id
        # 10万レコードから探索
        try:
            print(start_id, end_id)
            tmp, start_id = get_charge_next(start_id=start_id, end_id=end_id)
            output.extend(tmp)
        except :
            continue

    return output
    


if __name__ == '__main__':
    # sat_index = 15
    # start_year = 2000
    # end_year = 2008
    # InsertAll(sat_index=sat_index, start_year=start_year, end_year=end_year)
    # ReadChargeDate()
    # tmp, ind = get_charge_next(start_id=1, end_id=1000)
    res = GetChargeDataAll()
    breakpoint()