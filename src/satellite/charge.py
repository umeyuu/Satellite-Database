import os
from datetime import datetime

import cdflib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats as stats
import seaborn as sns
from matplotlib.colors import LogNorm


class SAT_Charge():

    def __init__(self) -> None:
        self.channel = np.array([ 30000, 20400, 13900, 9450, 6460, 4400, 3000,\
                                 2040, 1392, 949, 646, 440, 300, 204, 139, 95, 65, 44, 30], dtype=float)
        
        self.charge_range = list(range(7, 16))
        
    def open(self, path):
        _, extension = os.path.splitext(path)
        if extension == '.cdf':
            self.open_cdf(path=path)
        elif extension == '.csv':
            self.open_csv(path=path)
    
    # cdf を開く
    def open_cdf(self, path : str) -> None:
        cdf_file = cdflib.CDF(path)
        # イオン, エレクトロン
        self.ion = cdf_file['ION_DIFF_ENERGY_FLUX']
        self.electron = cdf_file['ELE_DIFF_ENERGY_FLUX']
        # 日にち
        epoch = cdflib.cdfepoch.unixtime(cdf_file['Epoch'])
        self.date = [datetime.utcfromtimestamp(e) for e in epoch]
        # 緯度経度（地磁気座標系）
        self.lat = abs(cdf_file['SC_AACGM_LAT'])
        self.lon = cdf_file['SC_AACGM_LTIME'] * np.pi / 12
    
    # csvを開く
    def open_csv(self, path : str):
        
        self.df = pd.read_csv(path, parse_dates=['date'])
        # イオン, エレクトロン
        self.ion = self.df.values[:, 27:46].astype(float)
        self.electron = self.df.values[:, 8:27].astype(float)
        # 日にち
        self.date = self.df.values[:, 0]
        # 緯度経度（地磁気座標系）
        self.lat = abs(self.df.mag_lat.values.astype(float))
        self.lon = self.df.mag_ltime.values.astype(float) * np.pi / 12

        
    # 表面帯電の概要をヒートマップで図示
    def heat_map(self, mode='I', st=67500):
        if mode == 'I':
            data = self.ion
            vmin = 1e3
            vmax = 1e8
            title = 'ION'
        elif mode == 'E':
            data = self.electron
            vmin = 1e5
            vmax = 1e10
            title = 'ELECTRON'

        et = st + 120
        df = pd.DataFrame(data.T[:, st:et], columns=self.date[st:et], index=self.channel)
        df_mask = (df == 0)
        plt.figure(figsize=(20,2))
        sns.heatmap(df, linewidths = 1, cmap = "jet", mask = df_mask, norm=LogNorm(vmin=vmin,vmax=vmax))
        plt.title(f'{title}_DIFF_ENERGY_FLUX')
        plt.xlabel('date', fontsize=15)
        plt.ylabel('energy [eV]', fontsize=15);

    # エネルギースペクトルを図示
    def plot_spectra(self, id=67520):
        plt.plot(self.channel, self.ion[id], marker='o', label='ION')
        plt.plot(self.channel, self.electron[id], marker='o', label='ELECTRON')
        plt.xscale('log')
        plt.yscale('log')
        plt.xlabel('Energy [eV]')
        plt.ylabel('Energy Flux [ev/cm2/delta-eV/ster/s]')
        plt.grid()
        plt.legend();

    # スミルノフ･グラブス検定
    def smirnov_grubbs(self, data, alpha):
        x, o = list(data), []
        while len(x) > 2:
            n = len(x)
            t = stats.t.isf(q=(alpha / n) / 2, df=n - 2)
            tau = (n - 1) * t / np.sqrt(n * (n - 2) + n * t * t)
            i_min, i_max = np.argmin(x), np.argmax(x)
            myu, std = np.mean(x), np.std(x, ddof=1)
            i_far = i_max if np.abs(x[i_max] - myu) > np.abs(x[i_min] - myu) else i_min
            tau_far = np.abs((x[i_far] - myu) / std)
            if tau_far < tau: 
                break
            o.append(x.pop(i_far))
        o.sort(reverse=True)
        return np.array(o)

    # 帯電している時間を検知
    def detect_charge(self):
        check_id_list = []
        alpha = 0.01
        charge_id = []
        # 14keV以上のelectronの流量が10^8以上
        for i, ele in enumerate(self.electron):
            if any(ele[:3] > 1e8):
                check_id_list.append(i)

        for i in check_id_list:
            check_ion = self.ion[i][self.ion[i] > 0]
            if len(check_ion) <= 2:
                continue

            # イオンの全チャンネルの値が大きい時
            if check_ion.mean() > 1e10:
                continue
            
            # 異常値検出
            out_array = self.smirnov_grubbs(check_ion, alpha)

            if len(out_array) == 0:
                continue

            for out_value in out_array:
                ch = np.where(self.ion[i] == out_value)[0].item()
                # 異常値は1e7以上で、95eV以上2040eV以下
                if ch in self.charge_range and out_value > 1e7:
                    charge_id.append((i, ch))
                    continue

        return charge_id

    # 帯電している位置を取得（地磁気座標系）
    def get_charge_pos(self):
        La = []
        Lo = []
        ind = self.detect_charge()
        for i, _ in ind:
            La.append(self.lat[i])
            Lo.append(self.lon[i])
        return La, Lo
    
    # 帯電している位置を図示
    def plot_charge_pos(self):
        La, Lo = self.get_charge_pos()
        ax = plt.subplot(111, projection="polar")
        ax.scatter(Lo, La)
        ax.set_ylim([90,40]);
    
    # 帯電チャンネルを追加. -1は帯電していない。
    def add_charge_col(self, save_path : str) -> None:
        try:
            length = len(self.df)
        except:
            pass

        charge_index_channel = self.detect_charge()
        channel = [-1] * length
        for i, ch in charge_index_channel:
            channel[i] = ch
        self.df['charge_channel'] = channel
        # 保存
        self.df.to_csv(save_path, index=False)


def main(index : int, start_year : int, end_year : int):
    sat = SAT_Charge()
    for year in range(start_year, end_year+1):
        for month in range(1, 13):
            for day in range(1, 32):
                print(year, month, day)

                month_str = str(month).zfill(2)
                day_str = str(day).zfill(2)
                save_dir = f'/Volumes/USB/Processed_Data/dmsp-f{index}/{year}/{month_str}/'
                save_file = f'dmsp-f{index}_{year}{month_str}{day_str}.csv'

                try :
                    sat.open(save_dir+save_file)
                    # 帯電情報を追加
                    sat.add_charge_col(save_path=save_dir+save_file)
                except:
                    continue

if __name__ == '__main__':
    index = 17
    start_year = 2007
    end_year = 2022
    main(index=index, start_year=start_year, end_year=end_year)


