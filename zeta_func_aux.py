import pandas as pd


def fiveminutal(df):
    print('fiveminutal')    
    return df.set_index('ts').asfreq('5T').interpolate('linear').reset_index()


def tenminutal(df):
    print('tenminutal')    
    return df.set_index('ts').asfreq('10T').interpolate('linear').reset_index()


def constant_fiveminutal(df):
    print('constant_fiveminutal')  
    primer_valor = df['value'][0] 
    return df.set_index('ts').asfreq('5T').ffill().reset_index().assign(
        value=lambda x: x['value'].shift(fill_value=primer_valor))


def constant_tenminutal(df):
    print('constant_tenminutal')
    primer_valor = df['value'][0]    
    return df.set_index('ts').asfreq('10T').ffill().reset_index().assign(
        value=lambda x: x['value'].shift(fill_value=primer_valor))


def accum_to_instant(acc_df, rain_var, freq, five_past_now):
    print('accum_to_instant')
    df = acc_df.copy()
    # Direct division based on the value of freq
    divisor = 12 if freq == '5M' else 6
    df['value'] /= divisor

    if rain_var == 'Rain_Acc_Daily':
        forecast_days = pd.date_range(start=five_past_now, periods=9, freq='D')
        return pd.concat([
            df[df['ts'].between(start, end, inclusive='left')]
            .assign(value=lambda df: df['value'].cumsum())
            for start, end in zip(forecast_days[:-1], forecast_days[1:])
        ], ignore_index=True)       
    else:
        return df
