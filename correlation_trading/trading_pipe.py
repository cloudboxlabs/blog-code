"""

A Dataflow pipeline that streams ingest real time tick prices for a set of stock tickers
and calculate pair-wise correlations in a 10 minutes sliding window. If the correlation
breaks certain threshold, stream output a trading signal to open a long/short position on the pair.

The underlying assumption of correlation trading is that similar stocks moves in tandem.
In statistical terms, their correlation should revert to mean.
We can pair trade them if the sliding window correlation breaks out of a threshold with the
assumption that we can close position once correlation reverts to mean.

"""

from __future__ import absolute_import

import argparse
import itertools
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms import window
import six

from transformers import calculate_correlation_pair, CorrelationPairDoFn, AddTimestampDoFn


SECONDS_IN_1_DAY = 3600 * 24
CORRELATION_THRESHOLD = -0.75
TICKER_LIST = ['goog', 'aapl', 'fb']
PAIRS = list(itertools.combinations(TICKER_LIST, 2))


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_mode',
        default='file',
        help='Streaming input or file based batch input')

    for ticker in TICKER_LIST:
        parser.add_argument(
            '--input_{}'.format(ticker),
            default='{}_hist.csv'.format(ticker),
            help='Cloud Pub/Sub topic of tick market data for a stock, fall back to flat csv')

    parser.add_argument('--output_topic',
                        default='/tmp/trading_signals.txt',
                        help='Topic of output trading signals in Google Cloud Pub/Sub')

    known_args, pipeline_args = parser.parse_known_args(argv)
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    if known_args.input_mode == 'stream':
        pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:

        # Read input
        input_stage = {}
        for ticker in TICKER_LIST:
            if known_args.input_mode == 'streaming':
                input_ticks = (p | beam.io.ReadFromPubSub(topic=known_args.input_topic)
                               .with_output_types(six.binary_type))
            else:
                input_ticks = (p | 'Read: %s' % ticker >> ReadFromText(
                                    getattr(known_args, 'input_%s' % ticker)))

            input_stage[ticker] = (input_ticks
                  | 'decode: %s' % ticker >> beam.Map(lambda x: x.decode('utf-8'))
                  | 'Filter: %s' % ticker >> beam.Filter(
                        lambda row: row.split(',')[0] != 'date')
                  | 'Add Timestamp: %s' % ticker >> beam.ParDo(AddTimestampDoFn())
                  | 'Window: %s' % ticker >> beam.WindowInto(
                       window.SlidingWindows(size=SECONDS_IN_1_DAY * 10, period=SECONDS_IN_1_DAY))
                  | 'Pair: %s' % ticker >> beam.ParDo(CorrelationPairDoFn(ticker)))

        # Group together all entries under the same ticker
        grouped = input_stage | 'group_by_name' >> beam.CoGroupByKey()

        correlations = (grouped
                        | 'Calculate pair correlation' >> beam.Map(calculate_correlation_pair))

        if known_args.input_mode == 'stream':
            trading_signals = (correlations | 'Filter correlation threshold' >> beam.Filter(
                                                lambda x: x[1] < CORRELATION_THRESHOLD)
                                            .with_output_types(six.binary_type))
            # pylint: disable=expression-not-assigned
            trading_signals | beam.io.WriteToPubSub(known_args.output_topic)
        else:
            trading_signals = (correlations | 'Filter correlation threshold' >> beam.Filter(
                                                lambda x: x[1] < CORRELATION_THRESHOLD))
            # pylint: disable=expression-not-assigned
            trading_signals | 'WriteOutput' >> WriteToText(known_args.output_topic)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
