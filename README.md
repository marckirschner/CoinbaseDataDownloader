# CoinbaseDataDownloader
Download Coinbase data and return either a pandas data frame or a save csv file

## Run from terminal

To download the last 10 days of ETH-USD 15 minute bar data and save to a CSV file, run the following:

``
python download_coinbase_data.py "API_KEY" "SECRET_KEY" "PASS_PHRASE"  --granularity=900 --market="ETH-USD" --num_days=10
``

## Import library
To return a pandas dataframe do the following

```python
from download_coinbase_data import CoinbaseDataDownloader

c = CoinbaseDataDownloader( "API_KEY", "SECRET_KEY", "PASS_PHRASE")
# Download remote data from Coinbase
df = c.get_data( granularity=900, market="ETH-USD", num_days=1)
# Read data from csv file
df = c.get_data( granularity=900, filename="ETH-USD_2020-04-30_2020-05-30.csv")
```