import pandas as pd


"""def fiveminutal(df):
    print('fiveminutal')
    df.set_index('ts').asfreq('5T').interpolate('linear').reset_index()
    print(df)
    return df


def tenminutal(df):
    print('tenminutal')
    df.set_index('ts').asfreq('10T').interpolate('linear').reset_index()
    print(df)
    return df


def constant_fiveminutal(df):
    print('constant_fiveminutal')
    df.set_index('ts').asfreq('5T').interpolate('linear').reset_index()
    print(df)
    return df



def constant_tenminutal(df):
    print('constant_tenminutal')
    primer_valor = df['value'][0]
    df.set_index('ts').asfreq('10T').ffill().reset_index().assign(
        value=lambda x: x['value'].shift(fill_value=primer_valor))
    print(df)
    return df



def accum_to_instant(acc_df, rain_var, freq, five_past_now):
    print('accum_to_instant')
    df = acc_df.copy()
    # Dividir 'value' según la frecuencia
    freq_divisors = {'5M': 12, '10M': 6}
    df.loc[:, 'value'] /= freq_divisors[freq]

    if rain_var == 'Rain_Acc_Daily':
        forecast_days = pd.date_range(start=five_past_now, periods=9, freq='D')
        df = pd.concat([
            df[df['ts'].between(start, end, inclusive='left')]
            .assign(value=lambda df: df['value'].cumsum())
            for start, end in zip(forecast_days[:-1], forecast_days[1:])
        ], ignore_index=True)
        print(df)
        return(df)
    else:
        return df"""

def fiveminutal(df):
    df.set_index('ts', inplace=True)
    df = df.asfreq(freq='5T')
    df = df.interpolate(method='linear')
    df = df.reset_index()
    return df


def tenminutal(df):
    df.set_index('ts', inplace=True)
    df = df.asfreq(freq='10T')
    df = df.interpolate(method='linear')
    df = df.reset_index()
    return df


def constant_fiveminutal(df):
    df.set_index('ts', inplace=True)
    df = df.asfreq(freq='5T', method='ffill')
    df = df.reset_index()
    primer_valor = df['value'][0]
    df['value'] = df['value'].shift(periods=1, fill_value=primer_valor)
    return df


def constant_tenminutal(df):
    df.set_index('ts', inplace=True)
    df = df.asfreq(freq='10T', method='ffill')
    df = df.reset_index()
    primer_valor = df['value'][0]
    df['value'] = df['value'].shift(periods=1, fill_value=primer_valor)
    return df

def accum_to_instant(acc_df, rain_var, freq, five_past_twelve):
    print('accum_to_instantesssssssss')
    instant_df = acc_df.copy()
    if freq == '5M':
        instant_df['value'] = instant_df['value'] / 12
    elif freq == '10M':
        instant_df['value'] = instant_df['value'] / 6
    
    if rain_var == 'Rain':
        return instant_df
    elif rain_var == 'Rain_Acc_Daily':
        forecast_days = pd.date_range(start=five_past_twelve, periods=9, freq='D')
        dfs_by_day = []
        for i in range(len(forecast_days) - 1):
            
            group = instant_df.loc[instant_df['ts'] >= forecast_days[i]]
            group = group.loc[group['ts'] < forecast_days[i + 1]]
            group['value'] = group['value'].cumsum()
            dfs_by_day.append(group)
        result_df = pd.concat(dfs_by_day, ignore_index=True)
        print(result_df)
        return result_df



