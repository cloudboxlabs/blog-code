import datetime
import itertools

import apache_beam as beam
import pandas as pd


TICKER_LIST = ['goog', 'aapl', 'fb']
PAIRS = list(itertools.combinations(TICKER_LIST, 2))


class CorrelationPairDoFn(beam.DoFn):
    """Parse each line of input text into words."""

    def __init__(self, ticker):
        super(CorrelationPairDoFn, self).__init__()
        self.ticker = ticker

    def process(self, element, *args, **kwargs):
        fields = element.split(',')
        dt, price = tuple(fields)[:2]

        for pair in PAIRS:
            if pair[0] == self.ticker:
                yield pair, (dt, price)
            elif pair[1] == self.ticker:
                yield pair, (dt, price)


class AddTimestampDoFn(beam.DoFn):

    def process(self, element, *args, **kwargs):
        trade_date = element.split(',')[0]
        unix_timestamp = int(datetime.datetime.strptime(trade_date, '%Y/%m/%d').strftime("%s"))
        yield beam.window.TimestampedValue(element, unix_timestamp)


def calculate_correlation_pair(element):
    pair, price_dict = element

    prices_1, prices_2 = price_dict[pair[0]], price_dict[pair[1]]

    if prices_1 and prices_2:
        ind, vals = zip(*prices_1)
        ser1 = pd.Series([float(v) for v in vals],
                         index=[pd.Timestamp(i) for i in ind])
        ind, vals = zip(*prices_2)
        ser2 = pd.Series([float(v) for v in vals],
                         index=[pd.Timestamp(i) for i in ind])

        return pair, ser1.corr(ser2)
    else:
        return pair, 0
