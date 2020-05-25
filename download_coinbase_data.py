"""
    Usage: python download_coinbase_data.py --api_key --secret_key --passcode --granularity --market --filename]

    Options:
        -h --help       Show this screen
        -a --api_key    Coinbase API Key
        -s --secret_key Coinbase Secret Key
        -p --passcode  Coinbase Passcode
        -g --granularity candle width in seconds, so granularity = 15 min = 15 * 60
        -n --num_days go back num_days of historical data
        -f --filename read this csv file instead (can be csv file output of CoinbaseDataDownloader.get_data)
        -m --market the currency pair, i.e. ETH-USD

    Example:
        python download_coinbase_data.py --api_key="apikey" --secret_key="secret_key" --passcode="passcode" --granularity=15*60 --market="ETH-USD"
"""

__author__ = "Marc J Kirschner"
__copyright__ = "Copyright (C) 2020 Marc J Kirschner"
__license__ = "Public Domain"
__version__ = "1.0"

from datetime import timedelta
from datetime import datetime

import time
import pandas as pd
import cbpro
from docopt import docopt

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
            data = self.api.get_product_historic_rates(market, granularity=15 * 60)
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
    args = docopt(__doc__)
    print(args)


if __name__ == '__main__':
    main()