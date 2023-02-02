import numpy as np

def read_binary_file(path : str) -> list:
    data = []
    with open(path, mode='rb') as f:
        while True:
            bytes=f.read(2)
            if bytes:
                data.append(int.from_bytes(bytes,byteorder='big'))
            else:
                break
    return data

from datetime import datetime, timedelta

delta_min = 2640
delta_sec = 43

# lat, geo_lat, mag_lat全ての緯度に使える
def get_latitude(lat : float) -> float:
    if lat < 1800:
        return float(lat-900)/10.0
    else:
        return float(lat-4995)/10.0 

# lon, geo_lon, mag_lon全ての経度に使える
def get_longitude(lon : float) -> float:
    return float(lon)/10.0


# チャンネルを昇順に並び替える
def rearrange_channel(input : list) -> list:
    output = []
    for i in range(0, 20, 4):
        tmp = input[i:i+4]
        tmp = list(reversed(tmp))
        output.extend(tmp)
    return output

# エネルギー流量に変換
def decompress_flux(energy_lis : list, gfactor : list, channel_lis : list) -> list:
    
    dt = 0.05
    output = []
    for ele, g, ch in zip(energy_lis, gfactor, channel_lis):
        X = ele % 32
        Y = (ele - X) / 32
        if (X + 32) * 2**Y -33 > 0:
            converted_value = (X + 32) * 2**Y / dt / g * ch
        else:
            converted_value = np.nan
        output.append(converted_value)
    return output
    


def convert_DataFrame(YMD : datetime, data : list):
    """
    YMD : datetime(year, month, day)
    """
    output = []
    for i in range(0, len(data), delta_min):

        DoY = data[0]

        lat = get_latitude(data[i+5])
        lon = get_longitude(data[i+6])

        
        for j in range(60):
            tmp = []
            base = 15 + i + delta_sec*j

            hour = data[base]
            minute = data[base+1]
            second = int(float(data[base + 2])/1000)

            date = YMD + timedelta(days=DoY-1, hours=hour, minutes=minute, seconds=second)

            electrons = rearrange_channel(data[base+3 : base+23])
            ions = rearrange_channel(data[base+23 : base+43])

            tmp.append(date)
            tmp.extend(electrons)
            tmp.extend(ions)
            output.append(tmp)
    
    return output
            