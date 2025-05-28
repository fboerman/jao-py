import pandas as pd
from .util import to_snake_case


def parse_final_domain(data: list[dict]) -> pd.DataFrame:
    # flatten the data
    # note: this only selects first CO
    # save the order of keys to keep output consistent
    # (note in p3.7+ dict order is guaranteed: https://stackoverflow.com/a/39980744

    columns = list(data[0].keys())
    # get keys of second level and insert them at right place in columns
    i = columns.index('contingencies')
    columns = columns[:i] + ['contingency_' + x for x in data[0]['contingencies'][0].keys()] + columns[i + 1:]
    for d in data:
        for c_k, c_d in d['contingencies'][0].items():
            d['contingency_' + c_k] = c_d
        del d['contingencies']
    # now build the dataframe and convert column names
    df = pd.DataFrame(data)
    df = df.rename(columns=lambda x: to_snake_case(x) if 'ptdf' not in x else x)
    # also convert our earlier column list
    columns = [to_snake_case(x) if 'ptdf' not in x else x for x in columns]
    # drop needless columns
    columns.remove('contingency_number')
    df = df.drop(columns=['contingency_number'])
    # fix order of columns
    df = df[columns]
    df = df.rename(columns={'id': 'id_original'})
    # parse datetime, convert to localtime and adjust column name
    df['date_time_utc'] = pd.to_datetime(df['date_time_utc'], utc=True).dt.tz_convert('europe/amsterdam')
    return df.rename(columns={'date_time_utc': 'mtu'})


def parse_monitoring(data: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(data)
    if 'businessDayUtc' in df:
        df['businessDay'] = pd.to_datetime(df['businessDayUtc'], utc=True).dt.tz_convert('europe/amsterdam').dt.date
        df = df.drop(columns=['businessDayUtc'])
    for c in ['deadline', 'lastModifiedOn']:
        df[c] = pd.to_datetime(df[c], utc=True).dt.tz_convert('europe/amsterdam')
    return df.drop(columns=['id'])


def parse_base_output(data: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(data).drop(columns='id')
    df['dateTimeUtc'] = pd.to_datetime(df['dateTimeUtc'], utc=True).dt.tz_convert('europe/amsterdam')
    df = df.set_index('dateTimeUtc')
    df.index.name = 'mtu'
    return df
