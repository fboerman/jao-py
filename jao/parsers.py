import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer


def _parse_utility_tool_xml(xml, t):
    """
    parses the xml coming out of the utility tool

    :param xml: string of the xml data
    :param t: which type to parse, choose from "MaxExchanges", "MaxNetPositions", "Ptdfs"
    :return:
    """
    # TODO: this is a pretty naieve version of parser. maybe etree directly or constrained soup is more efficient
    soup = BeautifulSoup(xml, 'lxml')
    selected_soup = soup.find(t)
    if selected_soup is None:
        return None
    # TODO