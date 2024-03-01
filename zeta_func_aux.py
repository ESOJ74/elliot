import pandas as pd

def fiveminutal9(df):
    print("fiveminutal")
    df.set_index('ts', inplace=True)
    print(df)
    df = df.asfreq(freq='5T')
    print(df)
    df = df.interpolate(method='linear')
    print(df)
    df = df.reset_index()
    print(df)
    print("fin fiveminutal")
    return df

def fiveminutal(df):    
    df.set_index('ts', inplace=True)
    # Cambiar la frecuencia a 5 minutos e interpolar
    df = df.asfreq('5T').interpolate('linear')
    # Restablecer índice y retornar el DataFrame
    return df.reset_index()

def constant_fiveminutal(df):
    print("constant_fiveminutal")
    df.set_index('ts', inplace=True)
    print(df)
    df = df.asfreq(freq='5T', method='ffill')
    print(df)
    df = df.reset_index()
    print(df)
    primer_valor = df['value'][0]
    df['value'] = df['value'].shift(periods=1, fill_value=primer_valor)
    print(df)
    print("fin_constant_fiveminutal")
    return df


def accum_to_instante(acc_df, rain_var, freq, five_past_now):
    print("accum_to_instant")
    instant_df = acc_df.copy()
    if freq == '5M':
        instant_df['value'] = instant_df['value'] / 12
    elif freq == '10M':
        instant_df['value'] = instant_df['value'] / 6

    if rain_var == 'Rain':
        return instant_df
    elif rain_var == 'Rain_Acc_Daily':
        forecast_days = pd.date_range(start=five_past_now, periods=9, freq='D')
        dfs_by_day = []
        for i in range(len(forecast_days) - 1):
            group = instant_df.loc[instant_df['ts'] >= forecast_days[i]]
            group = group.loc[group['ts'] < forecast_days[i + 1]]
            group['value'] = group['value'].cumsum()
            dfs_by_day.append(group)
        result_df = pd.concat(dfs_by_day, ignore_index=True)
        return result_df
    
def accum_to_instant(acc_df, rain_var, freq, five_past_now):
    instant_df = acc_df.copy()
    print('acum_to_instante')
    # Dividir 'value' según la frecuencia
    freq_divisors = {'5M': 12, '10M': 6}
    if freq in freq_divisors:
        instant_df['value'] /= freq_divisors[freq]

    if rain_var == 'Rain':
        return instant_df
    elif rain_var == 'Rain_Acc_Daily':
        forecast_days = pd.date_range(start=five_past_now,
                                      periods=9, freq='D')
        result_df = pd.concat([
            instant_df[instant_df['ts']
                       .between(start, end, inclusive='left')]
                       .assign(value=lambda df: df['value'].cumsum())
            for start, end in zip(forecast_days[:-1], forecast_days[1:])
        ], ignore_index=True)
        return result_df
