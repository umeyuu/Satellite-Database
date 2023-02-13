import cdflib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.colors import LogNorm
from datetime import datetime
import scipy.stats as stats
import os


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
        
        df = pd.read_csv(path, parse_dates=['date'])
        # イオン, エレクトロン
        self.ion = df.values[:, 27:].astype(float)
        self.electron = df.values[:, 8:27].astype(float)
        # 日にち
        self.date = df.values[:, 0]
        # 緯度経度（地磁気座標系）
        self.lat = abs(df.mag_lat.values.astype(float))
        self.lon = df.mag_ltime.values.astype(float) * np.pi / 12

        
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

