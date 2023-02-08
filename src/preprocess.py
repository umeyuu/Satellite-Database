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
            output.extend(tmp)
        return output


    # エネルギー流量に変換
    def calculate_flux(self, energy_lis : list, index : int, spicies : str) -> list:
        
        gfactor, channel_lis, delta_t = self.Sat_Conf.get(index, spicies)
        output = []
        for ele, g, ch in zip(energy_lis, gfactor, channel_lis):
            X = ele % 32
            Y = (ele - X) / 32
            if (X + 32) * 2**Y -33 > 0:
                converted_value = (X + 32) * 2**Y / delta_t / g * ch
            else:
                converted_value = np.nan
            output.append(converted_value)
        return output
        


    def convert_DataFrame(self, YMD : datetime, index : int):
        """
        YMD : datetime(year, month, day)
        """

        output = []
        for i in range(0, len(self.data), self.DELTA_MIN):

            DoY = self.data[0]

            lat = self.get_latitude(self.data[i+5])
            lon = self.get_longitude(self.data[i+6])

            
            for j in range(60):
                tmp = []
                base = 15 + i + self.DELTA_SEC * j

                hour = self.data[base]
                minute = self.data[base+1]
                second = int(float(self.data[base + 2])/1000)

                date = YMD + timedelta( hours=hour, minutes=minute, seconds=second)

                electrons = self.rearrange_channel(self.data[base+3 : base+23])
                ions = self.rearrange_channel(self.data[base+23 : base+43])

                # 流量
                ele_flux = self.calculate_flux(energy_lis=electrons, index=index, spicies='electron')
                ion_flux = self.calculate_flux(energy_lis=ions, index=index, spicies='ion')


                tmp.append(date)
                tmp.extend(ele_flux)
                tmp.extend(ion_flux)
                output.append(tmp)
        
        columns = ['date']
        electron_channel = self.Sat_Conf.electron_channel
        ion_channel = self.Sat_Conf.ion_channel
        chanels = electron_channel + ion_channel

        for i, ch in enumerate(chanels):
            if i <= len(electron_channel):
                spicies = 'electron'
            else:
                spicies = 'ion'
            columns.append(f'{spicies}_{ch}eV')
        

        return pd.DataFrame(output, columns=columns)
    
    def execute(self, YMD : datetime, index : int):
        """""
        YMD : 検索する日にち、　index : 衛星番号
        """""
        year = YMD.year
        month = str(YMD.month).zfill(2)
        day = str(YMD.day).zfill(2)
        
        path = f'/Volumes/USB/Raw_Data/dmsp-f{index}/{year}/{month}/dmsp-f{index}_{year}{month}{day}'
        self.read_binary_file(path=path)
        df = self.convert_DataFrame(YMD=YMD, index=index)
        return df

