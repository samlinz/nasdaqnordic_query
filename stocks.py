# Standard library imports.
import logging
import os
import pickle
from datetime import datetime
from enum import Enum

# Third party imports.
import bs4
import dateutil
import pandas as pd
import requests

# Default cache file directory. Will be created if does not exists and caches are used.
_DEFAULT_CACHE_DIR = 'cache'

# Create logger.
_log = logging.getLogger(__name__)

# Urls for Nasdaq XML API endpoints.
_API_URL = 'http://www.nasdaqomxnordic.com/webproxy/DataFeedProxy.aspx'


class Markets(Enum):
    """Enum containing the available markets."""

    # Nordic markets
    NORDIC_LARGE = 'L:INET:H7053910'
    NORDIC_MID = 'L:INET:H7053920'
    NORDIC_SMALL = 'L:INET:H7053930'

    # Copenhagen markets
    COPENHAGEN_LARGE = 'L:INET:H7096510'
    COPENHAGEN_MID = 'L:INET:H7096520'
    COPENHAGEN_SMALL = 'L:INET:H7096530'

    # Copenhagen markets
    STOCKHOLM_LARGE = 'L:INET:H7057510'
    STOCKHOLM_MID = 'L:INET:H7057520'
    STOCKHOLM_SMALL = 'L:INET:H7057530'

    # Helsinki
    HELSINKI_LARGE = 'L:INET:H7054310'
    HELSINKI_MID = 'L:INET:H7054320'
    HELSINKI_SMALL = 'L:INET:H7054330'


class MarketInstrument:
    """Represents a market instrument that prices can be queried for."""

    @classmethod
    def from_json_result(cls, result):
        instance = cls()

        float_properties = ['ask_price', 'bid_price', 'last_price', 'total_volume']

        for key, value in result.items():
            setattr(instance, key, float(value) if key in float_properties else value)

        return instance

    def __init__(self):
        self.id = None
        self.name = None
        self.full_name = None
        self.market = None
        self.bid_price = None
        self.ask_price = None
        self.last_price = None
        self.total_volume = None

    def __repr__(self) -> str:
        """Return a readable representation of the instrument."""
        properties = [x for x in dir(self) if
                      not x.startswith('_') and not callable(getattr(self, x))]
        property_strings = [f'{x}:{getattr(self, x, None)}' for x in properties]
        return ', '.join(property_strings)


def _fetch_stock_page(*markets) -> bs4.BeautifulSoup:
    """Query the instrument list page and return an XML soup."""

    if len(markets) == 0:
        raise ValueError('No markets given')

    params = {
        'Exchange' : 'NMF',
        'SubSystem': 'Prices',
        'Action'   : 'GetMarket',
        'app'      : '/osakkeet',
        'Market'   : ','.join([x.value for x in markets]),
        # 'ext_xslt': '/nordicV3/inst_table_shares.xsl'
    }

    r = requests.get(_API_URL, params)
    response_text = r.text
    soup = bs4.BeautifulSoup(response_text, 'lxml')

    return soup


def _parse_stock_instruments_response(soup: bs4.BeautifulSoup) -> dict:
    """Parse the XML response soup into dictionary with the relevant information."""

    # Get markets.
    markets = soup.find_all('market')
    if len(markets) == 0:
        raise ValueError('No markets found')

    instruments = []

    for market in markets:
        market_name = market.attrs['nm']
        _log.info(f'Processing market {market_name}')

        market_instruments = market.find('instruments')
        if len(market_instruments) == 0:
            _log.warning(f'Market {market_name} had no instruments!')
            continue

        for market_instrument in market_instruments:
            instr_attrs = market_instrument.attrs

            # Get instrument identifiers.
            instrument_id = instr_attrs['id']
            instrument_name = instr_attrs['nm']
            instrument_full_name = instr_attrs['fnm']

            # Get instrument details.
            instrument_bid_price = instr_attrs['bp']
            instrument_ask_price = instr_attrs['ap']
            instrument_last_price = instr_attrs['lp']
            instrument_total_volume = instr_attrs['tv']

            _log.info(f'Found instrument {instrument_name} id {instrument_id}')

            instruments.append({
                'id'          : instrument_id,
                'name'        : instrument_name,
                'full_name'   : instrument_full_name,
                'market'      : market_name,
                'bid_price'   : instrument_bid_price,
                'ask_price'   : instrument_ask_price,
                'last_price'  : instrument_last_price,
                'total_volume': instrument_total_volume
            })

    return instruments


def filter_market_instruments(instruments: [MarketInstrument], name_like: str) -> [
    MarketInstrument]:
    """Filter list of MarketInstrumennts by their shortened or full name and a partial string."""

    name_like = name_like.lower()

    result = []

    for instrument in instruments:
        matches = False

        instr_name = instrument.name.lower().strip()
        if name_like in instr_name:
            matches = True

        instr_full_name = instrument.full_name.lower().strip()
        if name_like in instr_full_name:
            matches = True

        if matches:
            result.append(instrument)

    return result


def _validate_dates(*dates):
    for date in dates:
        if isinstance(date, datetime):
            continue
        try:
            dateutil.parser.parse(date)
        except ValueError:
            raise ValueError(f'Invalid date string {date}')


def _get_cached_instrument_file(cache_files: [str], instrument_name: str, start_date: str,
                                end_date: str):
    parsed_start_date = dateutil.parser.parse(start_date)
    parsed_end_date = dateutil.parser.parse(end_date)

    for cache_file in cache_files:
        file_parts = cache_file.split('.')[0].split('_')
        if len(file_parts) != 3:
            continue

        cached_instrument_name = file_parts[0]
        cached_start_date = file_parts[1]
        cached_end_date = file_parts[2]

        if cached_instrument_name != instrument_name:
            continue

        try:
            parsed_cache_start_date = dateutil.parser.parse(cached_start_date)
            parsed_cache_end_date = dateutil.parser.parse(cached_end_date)
        except ValueError:
            _log.error(f'Failed to parse dates from cache file {cache_file}')
            continue

        if parsed_cache_start_date >= parsed_start_date and parsed_cache_end_date <= \
                parsed_end_date:
            return cache_file

    return None


def _create_dir_if_not_exists(dir: str):
    """Create (cache) directory if it does not exists."""

    if os.path.exists(dir) and not os.path.isdir(dir):
        raise ValueError(f'Provided path {dir} was not a directory')

    if not os.path.exists(dir):
        _log.info(f'Creating directory {dir}')
        os.mkdir(dir)


def _get_instrument_list_filename(markets: [Markets], date: datetime):
    """Get file name for instrument list cache file."""

    date_string = date.isoformat()[:10]
    markets_str = '_'.join(sorted([x.value[-4:] for x in markets]))

    return f'instruments_{date_string}_{markets_str}.data'


def _get_cached_instrument_list(markets: [Markets], date: datetime, cache_dir):
    cache_files = os.listdir(cache_dir)

    current_date_cache_file = _get_instrument_list_filename(markets, date)
    matching_cache_files = [x for x in cache_files if x == current_date_cache_file]

    return matching_cache_files[0] if any(matching_cache_files) else None


def _get_instrument_cache_file_path(instrument_name, start_date, end_date, cache_dir):
    """Get full file path for stock query cache file."""

    identifier = f'{instrument_name}_{start_date}_{end_date}'
    return os.path.join(cache_dir, f'{identifier}.data')


def get_stock_df(instrument_id: str
                 , start_date: any
                 , end_date: any
                 , load_from_cache=True
                 , save_to_cache=True
                 , cache_dir=_DEFAULT_CACHE_DIR
                 , return_only_df=True):
    """
    Query price history for a specific market item (stock).

    :param instrument_id: Instrument identifier, as returned by the market instrument query.
    :param start_date: Price date range start as ISO date string or datetime object.
    :param end_date: Price date range end as ISO date string or datetime object.
    :param load_from_cache: If true, allow loading from cache if the query range fits the cache
    file.
    :param save_to_cache: If true, store the result to cache file.
    :param cache_dir: Cache directory.
    :param return_only_df: If true, return only the DataFrame without company info.
    :return: Dictionary containing company information and Pandas DataFrame containing the stock
    price from the provided
    date range.
    """

    if not instrument_id.startswith('HEX'):
        raise ValueError(f'Invalid instrument name {instrument_id}')

    _validate_dates(start_date, end_date)

    # Convert datetimes to ISO format strings if they aren't already.
    start_date = start_date.isoformat()[:10] if isinstance(start_date, datetime) else start_date
    end_date = end_date.isoformat()[:10] if isinstance(end_date, datetime) else end_date

    file_path = _get_instrument_cache_file_path(instrument_id, start_date, end_date, cache_dir)

    if load_from_cache or save_to_cache:
        _create_dir_if_not_exists(cache_dir)

    if load_from_cache:
        cache_files = os.listdir(cache_dir)
        cached_file = _get_cached_instrument_file(cache_files, instrument_id, start_date,
                                                  end_date)

        if cached_file is not None:
            cached_file = os.path.join(_DEFAULT_CACHE_DIR, cached_file)
            _log.info('Loading from cache')

            with open(cached_file, 'rb') as f:
                cached_data = pickle.load(f)
                return cached_data['Value'] if return_only_df else cached_data

    params = {
        'SubSystem'      : 'History',
        'Action'         : 'GetChartData',
        'FromDate'       : start_date,
        'ToDate'         : end_date,
        'json'           : True,
        'showAdjusted'   : True,
        'app'            : '/osakkeet/historiallisetkurssitiedot-HistoryChar',
        'DefaultDecimals': False,
        'Instrument'     : instrument_id
    }

    r = requests.get(_API_URL, params)
    json_result = r.json()
    status = int(json_result['@status'])
    if status != 1:
        raise ValueError(f'Invalid status {status} or instrument {instrument_id}')

    json_data = json_result['data'][0]
    json_stock_name = json_data['instData']['@nm']
    json_company_name = json_data['instData']['@fnm']

    json_stock_value = json_data['chartData']['cp']

    # Create the stock DataFrame.
    pd_stock_value = pd.DataFrame(json_stock_value, columns=['Timestamp', 'Value'])

    # Create epoch timestamp and datetime columns.
    timestamps = pd_stock_value['Timestamp'].values // 1000
    pd_stock_value.loc[:, 'Timestamp'] = timestamps
    timestamps_dt = [datetime.fromtimestamp(x) for x in timestamps]
    pd_stock_value['DateTime'] = pd.to_datetime(timestamps_dt)

    result = {
        'Company': json_company_name,
        'Stock'  : json_stock_name,
        'Value'  : pd_stock_value
    }

    if save_to_cache:
        _log.info('Storing to cache')
        with open(file_path, 'wb+') as f:
            pickle.dump(result, f)

    return result['Value'] if return_only_df else result


def get_market_instruments(markets: [Markets]
                           , load_from_cache=True
                           , save_to_cache=True
                           , cache_dir=_DEFAULT_CACHE_DIR
                           , return_dict=False):
    """
    Get the market instruments from the provided markets.

    :param markets: List of market identifiers listed in Markets enum.
    :param load_from_cache: True if the result can be loaded from cache.
    :param save_to_cache: True if result of query is saved to cache.
    :param cache_dir: Cache directory.
    :param return_dict: If true, return raw dict, if false return MarketInstrument objects.
    :return: List of market instruments parsed from the API response.
    """

    if not isinstance(markets, list):
        raise ValueError(f'Markets must be a list, not {type(markets)}')

    if load_from_cache or save_to_cache:
        _create_dir_if_not_exists(cache_dir)

    # Load instrument list from cache if they were loaded for this date.
    if load_from_cache:
        cached_instruments = _get_cached_instrument_list(markets, datetime.now(), cache_dir)
        if cached_instruments is not None:
            _log.info('Loading from cache')

            cached_instruments_full_path = os.path.join(cache_dir, cached_instruments)

            with open(cached_instruments_full_path, 'rb') as f:
                cached_data = pickle.load(f)
                return cached_data if return_dict else [MarketInstrument.from_json_result(x) for x
                                                        in
                                                        cached_data]

    _log.debug('Fetching stock XML')
    stock_page = _fetch_stock_page(*markets)

    _log.debug('Parsing instruments')
    instruments = _parse_stock_instruments_response(stock_page)

    if save_to_cache:
        _log.debug('Storing instruments for this date')
        cached_instruments_filename = _get_instrument_list_filename(markets, datetime.now())

        cached_instruments_full_path = os.path.join(_DEFAULT_CACHE_DIR, cached_instruments_filename)

        with open(cached_instruments_full_path, 'wb+') as f:
            pickle.dump(instruments, f)

    # Return dict or convert the dicts into MarketInstrument objects.
    return instruments if return_dict else [MarketInstrument.from_json_result(x) for x in
                                            instruments]
