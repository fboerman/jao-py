import pandas as pd
from lxml import etree
from datetime import datetime
from .definitions import ParseDataSubject
from typing import Union


def _infer_and_convert_type(s):
    try:
        v = s[s.first_valid_index()]
    except KeyError:
        # no valid index so whole thing is nan. simply return as float nan type
        return s.astype(float)

    if type(v) != str:
        return s
    t = None
    try:
        int(v)
        t = int
    except:
        try:
            float(v)
            t = float
        except:
            if v == 'true' or v == 'false':
                return s.map({'true': True, 'false': False}).astype(bool)
    if t is not None:
        try:
            return s.astype(t)
        except:
            pass
    return s.fillna('')


def _parse_utility_tool_xml(xml: Union[bytes, str], t: ParseDataSubject) -> pd.DataFrame:
    """
    parses the xml coming out of the utility tool

    :param xml: string or bytes of the xml data
    :param t: which type to parse, choose from "MaxExchanges", "MaxNetPositions", "Ptdfs"
    :return:
    """
    if type(xml) == str:
        xml_b = xml.encode('UTF-8')
    elif type(xml) == bytes:
        xml_b = xml
    else:
        raise ValueError("xml should be provided in either bytes or string format")

    tree = etree.fromstring(xml_b)
    # the node name is the singular of the type
    node_name = t.value.strip('s')
    # get all the column names by checking all the children tags of a node
    column_names = [x.tag for x in tree.xpath(t + '/' + node_name)[0].getchildren()]

    # now get all data fast with xpath by retrieving the text for every tag, the order is guaranteed the same
    # so we can stitch them back together later
    data = []

    for c_name in column_names:
        data.append([str(x) for x in tree.xpath(f'{t}/{node_name}/{c_name}/node()')])

    df = pd.DataFrame(zip(*data), columns=column_names)
    for c in column_names[2:]:
        df[c] = _infer_and_convert_type(df[c])

    df['TIMESTAMP_CET'] = df.apply(lambda row: datetime.strptime(row['Date'], '%Y-%m-%dT00:00:00').replace(hour=int(row['CalendarHour'])-1), axis=1)

    df.drop(columns=['Date', 'CalendarHour'], inplace=True)

    return df


def _parse_utilitytool_xml(xml: Union[bytes, str], nodename: str, columnnames: list, datenode: str,
                           xpath: str='') -> pd.DataFrame:
    """
    parses the xml coming out excell utilitytool endpoints which is xml

    :param xml: string or bytes of the xml data
    :return:
    """

    if type(xml) == str:
        xml_b = xml.encode('UTF-8')
    elif type(xml) == bytes:
        xml_b = xml
    else:
        raise ValueError("xml should be provided in either bytes or string format")

    tree = etree.fromstring(xml_b)

    data = {
        'date': tree.xpath(xpath + f"ns:{nodename}/ns:{datenode}/node()", namespaces={'ns': 'http://tempuri.org/'}),
        'hour': tree.xpath(xpath + f"ns:{nodename}/ns:CalendarHour/node()", namespaces={'ns': 'http://tempuri.org/'}),
    }
    for c in columnnames:
        data[c] = tree.xpath(xpath + f"ns:{nodename}/ns:{c}/node()", namespaces={'ns': 'http://tempuri.org/'})

    df = pd.DataFrame.from_dict(data, orient='columns')
    df = df[['date', 'hour'] + columnnames]
    df[columnnames] = df[columnnames].astype('float')
    df = pd.DataFrame(df)

    # TODO: this fails with DST, change to index pd.date_range instead per day
    df['timestamp'] = df.apply(
        lambda row: datetime.strptime(row['date'], '%Y-%m-%dT00:00:00').replace(hour=int(row['hour']) - 1),
        axis=1)
    df.drop(columns=['date', 'hour'], inplace=True)
    df = df.set_index('timestamp').tz_localize('europe/amsterdam')

    return df


def _parse_maczt_final_flowbased_domain(df: pd.DataFrame, zone='NL') -> pd.DataFrame:
    """
    extracts the MACZT numbers from the final flowbased domain dataframe
    for now only NL zone is supported

    :param df:
    :return:
    """
    if zone != 'NL':
        raise NotImplementedError

    # select only relevant columns
    df = df[['CO', 'CO_EIC', 'CNE', 'CNE_EIC',
             'Presolved', 'RAM', 'Fmax', 'Fref', 'AMR', 'MinRAMFactor', 'MinRAMFactorJustification']]

    # if there is a default parameter day there is an empty dataframe. stop further processing
    if len(df) == 0:
        return df


    # filter on cnecs that have a valid dutch justification string and are not lta
    # make sure to make copy to prevent slice errors later
    df = df[df['MinRAMFactorJustification'].str.contains('MACZTtarget').fillna(False)]
    df = df[~(df['CNE'].str.contains('LTA_corner'))]
    df = pd.DataFrame(df)

    df['MCCC_PCT'] = 100 * df['RAM'] / df['Fmax']

    df[['MNCC_PCT', 'LF_CALC_PCT', 'LF_ACCEPT_PCT', 'MACZT_TARGET_PCT']] = \
        df['MinRAMFactorJustification'].str.extract(
            r'MNCC = (?P<MNCC_PCT>.*)%;LFcalc = (?P<LF_CALC_PCT>.*)%;LFaccept = (?P<LF_ACCEPT_PCT>.*)%;MACZTtarget = (?P<MACZT_TARGET_PCT>.*)%')
    df[['MCCC_PCT', 'MNCC_PCT', 'LF_CALC_PCT', 'LF_ACCEPT_PCT', 'MACZT_TARGET_PCT']] = \
        df[['MCCC_PCT', 'MNCC_PCT', 'LF_CALC_PCT', 'LF_ACCEPT_PCT', 'MACZT_TARGET_PCT']].astype(float)

    df['MACZT_PCT'] = df['MCCC_PCT'] + df['MNCC_PCT']
    df['LF_SUB_PCT'] = (df['LF_CALC_PCT'] - df['LF_ACCEPT_PCT']).clip(lower=0)
    df['MACZT_MIN_PCT'] = df['MACZT_TARGET_PCT'] - df['LF_SUB_PCT']
    df['MACZT_MARGIN'] = df['MACZT_PCT'] - df['MACZT_MIN_PCT']

    df.drop(columns=['MinRAMFactorJustification'], inplace=True)

    # there is only two decimals statistical relevance here so round it at that
    df[['MCCC_PCT', 'MACZT_PCT', 'LF_SUB_PCT', 'MACZT_MIN_PCT', 'MACZT_MARGIN']] = df[['MCCC_PCT', 'MACZT_PCT', 'LF_SUB_PCT', 'MACZT_MIN_PCT', 'MACZT_MARGIN']].round(2)

    return df


def _parse_suds_tradingdata(data, subject: str, nested: bool = False, freq: str = 'H') -> pd.DataFrame:
    """

    :param data: suds.sudsobject.TradingData object
    :param subject: string of the type that needs to be parsed
    :param freq: timefrequency for
    :return: resulting pandas dataframe
    """
    data_parsed = []
    if nested:
        data_raw = data[subject][subject.strip('s')]
    else:
        data_raw = data[subject]

    date_column = 'Date' if 'Date' in data_raw[0] else 'CalendarDate'

    df = pd.DataFrame([dict(x) for x in data_raw])
    df.index = pd.date_range(df[date_column].min(),
                             df[date_column].max().replace(hour=23),
                             freq=freq, tz='Europe/Amsterdam')
    df.drop(columns=[date_column, 'CalendarHour'], inplace=True)
    return df.astype(float)
