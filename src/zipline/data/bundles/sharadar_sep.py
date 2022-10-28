"""
Module for building a complete daily dataset from Quandl's Sharadar dataset.
"""
from io import BytesIO
import tarfile
from zipfile import ZipFile

from click import progressbar
from logbook import Logger
import pandas as pd
import requests
from six.moves.urllib.parse import urlencode
from six import iteritems
from trading_calendars import register_calendar_alias

from datetime import timedelta
import time


from zipline.utils.deprecate import deprecated
from zipline.data.bundles import core as bundles  # looking in .zipline/extensions.py
#from . import core as bundles
import numpy as np

# Code from:
    # Quantopian Zipline Issues:
    # "Cannot find data bundle during ingest #2275"
    #https://github.com/quantopian/zipline/issues/2275

log = Logger(__name__)

ONE_MEGABYTE = 1024 * 1024
QUANDL_DATA_URL = (
    'https://www.quandl.com/api/v3/datatables/SHARADAR/SEP.csv?'
)
@bundles.register('sharadar-prices')
def sharadar_prices_bundle(environ,
                  asset_db_writer,
                  minute_bar_writer,
                  daily_bar_writer,
                  adjustment_writer,
                  calendar,
                  start_session,
                  end_session,
                  cache,
                  show_progress,
                  output_dir):
    api_key = environ.get('QUANDL_API_KEY')
    if api_key is None:
        raise ValueError(
            "Please set your QUANDL_API_KEY environment variable and retry."
        )

    ###ticker2sid_map = {}
    start_time = time.time()
    raw_data = fetch_data_table(
        api_key, QUANDL_DATA_URL, False,
        show_progress,
        environ.get('QUANDL_DOWNLOAD_ATTEMPTS', 5)
    )
    
    # Quandl-Sharadar: 2021-03-29: Added full-up Adjusted Close, and moved Dividend to ACTIONS
    raw_data.rename(columns={'close': 'closetmp'}, inplace=True)
    raw_data.rename(columns={'closeadj': 'close'}, inplace=True)
    raw_data.rename(columns={'closetmp': 'closeua'}, inplace=True)
    
    asset_metadata = gen_asset_metadata(
        raw_data[['symbol', 'date']],
        show_progress
    )
    asset_db_writer.write(asset_metadata)

    symbol_map = asset_metadata.symbol
    sessions = calendar.sessions_in_range(start_session, end_session)

    raw_data.set_index(['date', 'symbol'], inplace=True)
    daily_bar_writer.write(
        parse_pricing_and_vol(
            raw_data,
            sessions,
            symbol_map
        ),
        show_progress=show_progress
    )
    adjustment_writer.write() # Don't need adjustments anymore, yet need to register empty tables.
    print(f"--- Time to ingest bundle: sharadar-prices :{timedelta(seconds=time.time() - start_time)} ---")



def format_metadata_url(url, api_key):
    """ Build the query URL for Quandl Prices metadata.
    """
    query_params = [('api_key', api_key), ('qopts.export', 'true')]

    return (
        url + urlencode(query_params)
    )


def load_data_table(file,
                    index_col,
                    is_action=False,
                    show_progress=False):
    """ Load data table from zip file provided by Quandl.
    """
    with ZipFile(file) as zip_file:
        file_names = zip_file.namelist()
        assert len(file_names) == 1, "Expected a single file from Quandl."
        wiki_prices = file_names.pop()
        with zip_file.open(wiki_prices) as table_file:
            if show_progress:
                log.info('Parsing raw data.')
                
            if not is_action:
                cols=[
                    'ticker',
                    'date',
                    'open',
                    'high',
                    'low',
                    'close',
                    'volume',
                    'closeadj',
                    ###As of 2021-03-29 'dividends' is in another table, and has been replaced by 'closeadj',
                    ##'closeunadj',
                    ##'lastupdated' #prune last two columns for zipline bundle load
                ]
            else:
                cols=[
                    'date',
                    'action',
                    'ticker',
                    'value',
                ]
            data_table = pd.read_csv(
                table_file,
                parse_dates=['date'],
                index_col=index_col,
                usecols=cols,
            )

            if not is_action:
                col_rename_dict={
                    'ticker': 'symbol'
                }
            else:
                col_rename_dict={
                    'ticker': 'symbol',
                    'value': 'dividend',
                }
    data_table.rename(
        columns=col_rename_dict,
        inplace=True,
        copy=False,
    )
    
    return data_table

#url_full_action_tbl = format_metadata_url(QUANDL_ACTIONS_URL, api_key)

def fetch_data_table(api_key, url, is_action,
                     show_progress,
                     retries):
    for _ in range(retries):
        try:
            if show_progress:
                log.info('Downloading Sharadar Price/Action metadata.')
            url_full_tbl = format_metadata_url(url, api_key)
            #ajjc: Debug only: log.info('url_full_tbl={}'.format(url_full_tbl)) # ajjc:Note: Need QUANDL_API_KEY to not have quotes around it in environment.
            metadata = pd.read_csv(url_full_tbl

            )
            # Extract link from metadata and download zip file.
            table_url = metadata.loc[0, 'file.link']
            #ajjc: Debug only: log.info('url_meta={}'.format(table_url)) # ajjc: Note: Security protocol says not to print API key/access.

            if show_progress:
                raw_file = download_with_progress(
                    table_url,
                    chunk_size=ONE_MEGABYTE,
                    label="Downloading Prices table from Quandl Sharadar"
                )
            else:
                raw_file = download_without_progress(table_url)

            return load_data_table(
                file=raw_file,
                index_col= None,
                is_action=is_action,
                show_progress=show_progress,
            )

        except Exception:
            log.exception("Exception raised reading Quandl data. Retrying.")

    else:
        raise ValueError(
            "Failed to download Quandl data after %d attempts." % (retries)
        )


def gen_asset_metadata(data, show_progress):
    if show_progress:
        log.info('Generating asset metadata.')

    data = data.groupby(
        by='symbol'
    ).agg(
        {'date': [np.min, np.max]}
    )
    data.reset_index(inplace=True)
    data['start_date'] = data.date.amin
    data['end_date'] = data.date.amax
    del data['date']
    data.columns = data.columns.get_level_values(0)

    data['exchange'] = 'QUANDL'
    data['auto_close_date'] = data['end_date'].values + pd.Timedelta(days=1)
    return data


def parse_pricing_and_vol(data,
                          sessions,
                          symbol_map):
    for asset_id, symbol in iteritems(symbol_map):
        asset_data = data.xs(
            symbol,
            level=1
        ).reindex(
            sessions.tz_localize(None)
        ).fillna(0.0)
        yield asset_id, asset_data


def download_with_progress(url, chunk_size, **progress_kwargs):
    """
    Download streaming data from a URL, printing progress information to the
    terminal.

    Parameters
    ----------
    url : str
        A URL that can be understood by ``requests.get``.
    chunk_size : int
        Number of bytes to read at a time from requests.
    **progress_kwargs
        Forwarded to click.progressbar.

    Returns
    -------
    data : BytesIO
        A BytesIO containing the downloaded data.
    """
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    total_size = int(resp.headers['content-length'])
    log.info('tbl:total_size={}'.format(total_size))
    data = BytesIO()
    with progressbar(length=total_size, **progress_kwargs) as pbar:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            data.write(chunk)
            pbar.update(len(chunk))

    data.seek(0)
    return data


def download_without_progress(url):
    """
    Download data from a URL, returning a BytesIO containing the loaded data.

    Parameters
    ----------
    url : str
        A URL that can be understood by ``requests.get``.

    Returns
    -------
    data : BytesIO
        A BytesIO containing the downloaded data.
    """
    resp = requests.get(url)
    resp.raise_for_status()
    return BytesIO(resp.content)

#register_calendar_alias("QUANDL", "NYSE")
register_calendar_alias("sharadar-prices", "NYSE")

