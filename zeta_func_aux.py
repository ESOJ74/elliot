import pandas as pd


def fiveminutal(df):
    return df.set_index('ts').asfreq('5T').interpolate('linear').reset_index()


def tenminutal(df):
    return df.set_index('ts').asfreq('10T').interpolate('linear').reset_index()


def constant_fiveminutal(df):
    return df.set_index('ts').asfreq('5T').interpolate('linear').reset_index()



def constant_tenminutal(df):
    primer_valor = df['value'][0]
    return df.set_index('ts').asfreq('10T').ffill().reset_index().assign(
        value=lambda x: x['value'].shift(fill_value=primer_valor))



def accum_to_instant(acc_df, rain_var, freq, five_past_now):
    # Dividir 'value' según la frecuencia
    freq_divisors = {'5M': 12, '10M': 6}
    acc_df['value'] /= freq_divisors[freq]

    if rain_var == 'Rain_Acc_Daily':
        forecast_days = pd.date_range(start=five_past_now, periods=9, freq='D')
        return pd.concat([
            acc_df[acc_df['ts'].between(start, end, inclusive='left')]
            .assign(value=lambda df: df['value'].cumsum())
            for start, end in zip(forecast_days[:-1], forecast_days[1:])
        ], ignore_index=True)
    else:
        return acc_df


