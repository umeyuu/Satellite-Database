import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from src.parameter import Sat_Config


class Process_Binary_File(Sat_Config):
    def __init__(self) -> None:
        self.DELTA_MIN = 2640
        self.DELTA_SEC = 43

        self.Sat_Conf = Sat_Config()


    # バイナリーファイルを読み込む
    def read_binary_file(self, path : str) -> list:
        self.data = []
        with open(path, mode='rb') as f:
            while True:
                bytes=f.read(2)
                if bytes:
                    self.data.append(int.from_bytes(bytes,byteorder='big'))
                else:
                    break

    # lat, geo_lat, mag_lat全ての緯度に使える
    def get_latitude(self, lat : float) -> float:
        if lat < 1800:
            return float(lat-900)/10.0
        else:
            return float(lat-4995)/10.0 

    # lon, geo_lon, mag_lon全ての経度に使える
    def get_longitude(self, lon : float) -> float:
        return float(lon)/10.0


    # チャンネルを昇順に並び替える
    def rearrange_channel(self, input : list) -> list:
        output = []
        for i in range(0, 20, 4):
            tmp = input[i:i+4]
            tmp = list(reversed(tmp))
            if i == 8:
                del tmp[2]
            output.extend(tmp)
        return output


    # エネルギー流量に変換
    def calculate_flux(self, energy_lis : list, index : int) -> list:
        
        gfactor, channel_lis, delta_t = self.Sat_Conf.get(index)

        output = []
        for energy, g, ch in zip(energy_lis, gfactor, channel_lis):
            X = energy % 32
            Y = (energy - X) / 32
            count = (X + 32) * 2**Y -33
            if  count > 0:
                converted_value = count / delta_t / g * ch
            else:
                converted_value = 0.0
            output.append(converted_value)
        return output
    
    # 3種類の緯度経度を取得
    def get_lat_lon(self, index : int):
        lat = self.get_latitude(self.data[index + 5]) # 測地学的緯度
        lon = self.get_longitude(self.data[index + 6]) # 測地学的経度

        geo_lat = self.get_latitude(self.data[index + 8]) # 地理座標系の緯度
        geo_lon = self.get_longitude(self.data[index + 9]) # 地理座標系の経度

        mag_lat = self.get_latitude(self.data[index + 10]) # 地磁気緯度
        mag_lon = self.get_longitude(self.data[index + 11]) # 地磁気経度
        mag_ltime = self.data[index + 12] #地磁気現地時間

        return np.array([lat, lon, geo_lat, geo_lon, mag_lat, mag_lon, mag_ltime])


    def convert_DataFrame(self, YMD : datetime, index : int):
        """
        YMD : datetime(year, month, day)
        """

        output = []
        length = len(self.data)
        for i in range(0, length, self.DELTA_MIN):
            # 現在の緯度経度
            latlon_array = self.get_lat_lon(index=i)
            
            if i != length - self.DELTA_MIN:
                # 1分後の緯度経度
                next_latlon_array = self.get_lat_lon(index=i+self.DELTA_MIN)
                # 1秒間の緯度経度の変化量
                delta_latlon_array = (next_latlon_array - latlon_array) / 60
            

            for j in range(60):
                tmp = []
                base = 15 + i + self.DELTA_SEC * j

                # 時間
                hour = self.data[base]
                minute = self.data[base+1]
                second = int(float(self.data[base + 2])/1000)
                date = YMD + timedelta( hours=hour, minutes=minute, seconds=second)

                # 緯度、経度
                current_latlon = list(latlon_array + delta_latlon_array * j)

                # センサ値
                electrons = self.rearrange_channel(self.data[base+3 : base+23])
                ions = self.rearrange_channel(self.data[base+23 : base+43])

                # 流量
                ele_flux = self.calculate_flux(energy_lis=electrons, index=index)
                ion_flux = self.calculate_flux(energy_lis=ions, index=index)


                tmp.append(date)
                tmp.extend(current_latlon)
                tmp.extend(ele_flux)
                tmp.extend(ion_flux)
                output.append(tmp)
        
        # 列名を定義
        columns = ['date', 'lat', 'lon', 'geo_lat', 'geo_lon', 'mag_lat', 'mag_lon', 'mag_ltime']
        chanel = self.Sat_Conf.channel
        chanels = chanel + chanel

        for i, ch in enumerate(chanels):
            if i < len(chanel):
                spicies = 'electron'
            else:
                spicies = 'ion'
            columns.append(f'{spicies}_{ch}eV')
        

        return pd.DataFrame(output, columns=columns)
    
    def execute(self, YMD : datetime, index : int):
        """""
        YMD : 検索する日にち、 index : 衛星番号
        """""
        year = YMD.year
        month = str(YMD.month).zfill(2)
        day = str(YMD.day).zfill(2)
        
        path = f'/Volumes/USB/Raw_Data/dmsp-f{index}/{year}/{month}/dmsp-f{index}_{year}{month}{day}'
        self.read_binary_file(path=path)
        df = self.convert_DataFrame(YMD=YMD, index=index)
        return df

