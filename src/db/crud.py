from setting import ENGINE, session
import pandas as pd
import numpy as np
from datetime import datetime
from models import Charge_Sat



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
                print(year, month, day)

                month_str = str(month).zfill(2)
                day_str = str(day).zfill(2)
                path = f'/Volumes/USB/Processed_Data/dmsp-f{sat_index}/{year}/{month_str}/dmsp-f{sat_index}_{year}{month_str}{day_str}.csv'

                # データベースに挿入
                try :
                    InsertChargeData(path=path, sta_index=sat_index)
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
    df.to_csv('../../charge.csv', index=False)

        


if __name__ == '__main__':
    # sat_index = 16
    # start_year = 2004
    # end_year = 2022
    # InsertAll(sat_index=sat_index, start_year=start_year, end_year=end_year)
    ReadChargeDate()
