import pandas as pd
from lxml import etree
from datetime import datetime


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


def _parse_utility_tool_xml(xml, t):
    """
    parses the xml coming out of the utility tool

    :param xml: string of the xml data
    :param t: which type to parse, choose from "MaxExchanges", "MaxNetPositions", "Ptdfs"
    :return:
    """
    tree = etree.fromstring(xml.encode('UTF-8'))
    # the node name is the singular of the type
    node_name = t.strip('s')
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