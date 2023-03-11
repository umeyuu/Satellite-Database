from datetime import datetime
from typing import Tuple

import numpy as np
import pandas as pd
from models import Charge_Sat
from setting import ENGINE, session
from sqlalchemy import func, or_


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
    mag_ltime_array = df.mag_ltime.resample('MIN').first().values
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
def get_charge_next(satellite_id : int, start_id: int, end_id : int) -> Tuple[list, int]:
    # サブクエリー
    subquery = (session.query(
        Charge_Sat.satellite_id,
        Charge_Sat.date,
        Charge_Sat.lat,
        Charge_Sat.lon,
        Charge_Sat.charge_count,
        func.lead(Charge_Sat.charge_count, 1).over(order_by=Charge_Sat.date).label('one_minute_after'),
        func.lead(Charge_Sat.charge_count, 2).over(order_by=Charge_Sat.date).label('two_minute_after'),
        func.lead(Charge_Sat.charge_count, 3).over(order_by=Charge_Sat.date).label('three_minute_after'),
        func.lead(Charge_Sat.charge_count, 4).over(order_by=Charge_Sat.date).label('four_minute_after'),
        func.lead(Charge_Sat.charge_count, 5).over(order_by=Charge_Sat.date).label('five_minute_after'),
        func.lead(Charge_Sat.charge_count, 6).over(order_by=Charge_Sat.date).label('six_minute_after'),
        func.lead(Charge_Sat.charge_count, 7).over(order_by=Charge_Sat.date).label('seven_minute_after'),
        func.lead(Charge_Sat.charge_count, 8).over(order_by=Charge_Sat.date).label('eight_minute_after'),
        func.lead(Charge_Sat.charge_count, 9).over(order_by=Charge_Sat.date).label('nine_minute_after'),
        func.lead(Charge_Sat.charge_count, 10).over(order_by=Charge_Sat.date).label('ten_minute_after'),
    ).filter(
        Charge_Sat.id.between(start_id, end_id)
        )).subquery('sub')
    
    # メインクエリー
    response = session.query(subquery).filter(or_(
        subquery.c.one_minute_after > 0, 
        subquery.c.two_minute_after > 0,
        subquery.c.three_minute_after > 0,
        subquery.c.four_minute_after > 0,
        subquery.c.five_minute_after > 0,
        subquery.c.six_minute_after > 0,
        subquery.c.seven_minute_after > 0,
        subquery.c.eight_minute_after > 0,
        subquery.c.nine_minute_after > 0,
        subquery.c.ten_minute_after > 0,
        )).all()

    output = []
    for res in response:
        tmp = []
        for col in res:
            tmp.append(col)
        output.append(tmp)
    
    next_ind = end_id + 1
    return output, next_ind

# 任意の衛星の帯電データを全て取得
def GetChargeDataBySatellite(satellite_id : int) -> list:
    start_id = session.query(Charge_Sat.id).filter(
        Charge_Sat.satellite_id == satellite_id
    ).first()[0]

    last_id = session.query(Charge_Sat.id).filter(
        Charge_Sat.satellite_id == satellite_id
        ).order_by(
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

# dmsp-f16~f18の帯電データを全て取得
def GetChargeDataAll():
    output = []
    for i in range(16, 19):
        tmp = GetChargeDataBySatellite(satellite_id=i)
        output.extend(tmp)
    columns = ['satellite_id', 'date', 'lat', 'lon', 'charge_count','one_minute_after', 
               'two_minute_after', 'three_minute_after', 'four_minute_after','five_minute_after', 
               'six_minute_after', 'seven_minute_after', 'eight_minute_after', 'nine_minute_after', 'ten_minute_after']
    df = pd.DataFrame(output, columns=columns)
    df.to_csv('charge.csv', index=False)

    
def UpdateChargeCountByDate(path : str, start_id : int) -> int:
    # csvを読み込み
    df = pd.read_csv(path, parse_dates=['date'])
    charge_count_array = df.charge_channel.resample('MIN').apply(charge_count).values
    # DBの読み込む最後のid
    end_id = len(charge_count_array) + start_id    
    
    # クエリー
    responses = session.query(
        Charge_Sat
    ).filter(
        Charge_Sat.id.between(start_id, end_id)
    ).all()

    # 更新
    for response, value in zip(responses, charge_count_array):
        response.charge_count = value
    session.commit()

    return end_id


def UpdateChargeCount(sat_index : int, start_year :int, end_year : int) -> None:
    end_id = 0 # 初期値

    for year in range(start_year, end_year+1):
        for month in range(1, 13):
            for day in range(1, 32):

                month_str = str(month).zfill(2)
                day_str = str(day).zfill(2)
                path = f'/Volumes/USB/Processed_Data/dmsp-f{sat_index}/{year}/{month_str}/dmsp-f{sat_index}_{year}{month_str}{day_str}.csv'
                start_id = get_date_id(satellite_id=sat_index, YMD=datetime(year=year, month=month, day=day))

                # charge_countを更新
                try :
                    if end_id != 0 and start_id != end_id+1:
                        raise 'start_idが間違っている'
                    end_id = UpdateChargeCountByDate(path=path, start_id=start_id)
                except:
                    print(year, month, day)
                    continue




if __name__ == '__main__':
    sat_index = 17
    start_year = 2007
    end_year = 2022
    InsertAll(sat_index=sat_index, start_year=start_year, end_year=end_year)
    # ReadChargeDate()
    # tmp, ind = get_charge_next(satellite_id=16, start_id=4184097, end_id=4184097+100000)
    # res = GetChargeDataAll(satellite_id=16)
    # breakpoint()