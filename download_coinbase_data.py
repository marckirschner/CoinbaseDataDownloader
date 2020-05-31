"""Usage:
  download_coinbase.py <api> <secret> <pass> --market=<market> --granularity=granularity --num_days=num_days [--filename=filename] [-v]
  download_coinbase.py -h | --help | --version
"""

__author__ = "Marc J Kirschner"
__copyright__ = "Copyright (C) 2020 Marc J Kirschner"
__license__ = "Public Domain"
__version__ = "0.1.1rc"

import sys
from docopt import docopt
from datetime import timedelta
from datetime import datetime
import time
import pandas as pd
import cbpro


class CoinbaseDataDownloader:
    def __init__(self, api_key, secret_key, passcode):
        self.api = cbpro.AuthenticatedClient(api_key, secret_key, passcode)

    """
        @param granularity candle width in seconds, so granularity = 15 min = 15 * 60
        @param num_days go back num_days of historical data
        @filename if filename is None then read data from coinbase for currency pair @market
                  if filename is not none then read the csv file and return a data frame
        @market the currency pair, i.e. ETH-USD
        @cointime number of seconds to wait between each request made to coinbase.
                  coinbase rate limits and will return an error if you exceed a threshold
                  of requests in an allotted amount of time
    """
    def get_data(self, granularity=15 * 60, num_days=30, filename=None, market="ETH-USD", cointime=0.5):
        if filename is None:
            end = datetime.now()
            start = end - timedelta(days=num_days)
            start_cx = start
            new_end = start + timedelta(days=2)
            frames = []
            while start_cx < end:
                start_query = start_cx.strftime("%Y-%m-%d")
                end_query = new_end.strftime("%Y-%m-%d")
                data = self.api.get_product_historic_rates(market, granularity=granularity, start=start_query, end=end_query)
                df = pd.DataFrame(data, columns=["Time", "Low", "High", "Open", "Close", "Vol"])
                df = df.reindex(index=df.index[::-1])
                df = df.reset_index()
                df['Time2'] = df['Time'].apply(
                    lambda x: datetime.strftime(datetime.fromtimestamp(float(x)), '%Y-%m-%d %H:%M'))
                frames.append(df)
                start_cx = new_end
                new_end = start_cx + timedelta(days=2)
                time.sleep(cointime)

            time.sleep(cointime)
            data = self.api.get_product_historic_rates(market, granularity=granularity)
            df = pd.DataFrame(data, columns=["Time", "Low", "High", "Open", "Close", "Vol"])
            df = df.reindex(index=df.index[::-1])
            df = df.reset_index()
            df['Time2'] = df['Time'].apply(
                lambda x: datetime.strftime(datetime.fromtimestamp(float(x)), '%Y-%m-%d %H:%M'))

            cut_point = df[df['Time2'] == frames[-1]['Time2'].iloc[-1]].index.values[0]
            df = df.iloc[cut_point + 1:]
            frames.append(df)

            final_df = pd.concat(frames)
            final_df['Time2'] = final_df['Time'].apply(
                lambda x: datetime.strftime(datetime.fromtimestamp(float(x)), '%Y-%m-%d %H:%M'))
            final_df = final_df.reset_index()
            final_df.to_csv(f"{market}_{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.csv")

            return final_df
        else:
            final_df = pd.read_csv(filename)
            return final_df


def main():
    args = docopt(__doc__, version=__version__)

    api_key = args['<api>']
    secret_key = args['<api>']
    passcode = args['<pass>']
    filename = args['--filename']
    market = args['--market']
    granularity = args['--granularity']
    num_days = args['--num_days']

    try:
        granularity = int(granularity)
        num_days = int(num_days)
    except Exception as e:
        print(f"Invalid parameter:, {e}")
        sys.exit()

    cdd =CoinbaseDataDownloader(api_key, secret_key, passcode)
    cdd.get_data(granularity=granularity, num_days=num_days, filename=filename, market=market, cointime=0.5)

if __name__ == '__main__':
    main()
