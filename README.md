# Nordic markets queries

Tiny Python library I wrote when toying around with stock data from Nordic markets.

I reverse engineered the queries to Nasdaq's XML API and wrote a few helper functions
and classes to make querying companies, filtering them and getting stock price information
from given timespans easy.

* Query all companies from given market or markets with *get_market_instruments*
* Markets with their identifiers are listed in *Markets* enum class
* Filter out companies with by matching string to name with *filter_market_instruments*
* Query stock price from date range with *get_stock_df*

*get_stock_df* returns a Pandas DataFrame with price and time columns.

All of the results are cached by default
* Market instruments query is valid for a day and the same data will be queried only once a day
* Stock price data will be cached and subsequent queries will use cached data if date range falls into the range

## Example: Get companies, filter out all except Outokumpu and plot stock price using Matplotlib.

```python
# Fetch all stocks from the provided market(s).
instruments = get_market_instruments([Markets.HELSINKI_LARGE])

# Filter out everything except the company/companies of interest.
# Note that all instruments will be cached for the current date.
instruments_filtered = filter_market_instruments(instruments, 'outokumpu')

if not instruments_filtered:
    print('No instruments found')
    sys.exit()

instrument = instruments_filtered[0]

# Fetch stock price from the provided time range.
data = get_stock_df(instrument.id, '2018-01-01', '2018-06-01')

# Plot the stock price over time.
plt.plot(data['DateTime'], data['Value'], label=instrument.full_name)
plt.legend()
plt.show()

```

## Dependencies

```bash
pip install bs4 dateutil pandas requests
```