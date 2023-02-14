import urllib.request
import os
from datetime import datetime
import subprocess


# dmspデータのcdfファイルを取得
def save_satelite_cdf_data(name):
    for year in range(2010, 2015):
        for month in range(1, 13):
            for day in range(1, 32):
                year = str(year)
                month = str(month).zfill(2)
                day = str(day).zfill(2)
                path = f'https://www.ncei.noaa.gov/data/dmsp-space-weather-sensors/access/{name}/ssj/{year}/{month}/dmsp-{name}_ssj_precipitating-electrons-ions_{year}{month}{day}_v1.1.2.cdf'
                # 外付けハードディスクに保存
                save_dir = f'/Volumes/USB/Raw_Data/dmsp-{name}/{year}/{month}/'
                file_name = f'dmsp-{name}_{year}{month}{day}.cdf' 

                if not os.path.isdir(save_dir):
                    os.makedirs(save_dir)
                try:
                    urllib.request.urlretrieve(path, save_dir+file_name)
                except:
                    print(f'{file_name}の保存に失敗しました。')
                    continue

# 任意の日にちが1月1日から何日目か返す関数
def get_day_of_year(year : int, month : int, day : int) -> int:
    try:
        dt = datetime(year, month, day)
        sdate=datetime(year,1,1)
        doy_time = dt - sdate
        return doy_time.days + 1
    except:
        return None

# .gzファイルを解凍する
def decompress(path : str):
    cmd = ['gunzip', path]
    subprocess.run(cmd)

# cdfデータがないdmspデータを取得する
def scrape_dmsp(name : str, st_year : int, et_year : int):
    for year in range(st_year, et_year):
        y = str(year)
        for month in range(1, 13):
            m = str(month).zfill(2)
            for day in range(1, 32):
                date = get_day_of_year(year, month, day)
                if date == None:
                    continue
                date = str(date).zfill(3)
                d = str(day).zfill(2)
                path = f'https://www.ncei.noaa.gov/data/dmsp-space-weather-sensors/access/{name}/ssj/{y}/{m}/j5{name}{y[2:]}{date}.gz'
                # 外付けハードディスクに保存
                save_dir = f'/Volumes/USB/Raw_Data/dmsp-{name}/{y}/{m}/'
                file_name = f'dmsp-{name}_{y}{m}{d}.gz'
                
                # ディレクトリー作成
                if not os.path.isdir(save_dir):
                    os.makedirs(save_dir)
                # 保存
                try:
                    urllib.request.urlretrieve(path, save_dir+file_name)
                    # 解凍
                    decompress(save_dir+file_name)
                except:
                    print(f'{file_name}の保存に失敗しました。')
                    continue
            


if __name__ == '__main__':
    # save_satelite_cdf_data('f16')
    scrape_dmsp('f18', 2010, 2015)
