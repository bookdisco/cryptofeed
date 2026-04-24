'''
Copyright (C) 2017-2025 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import random

import pytest

from cryptofeed.defines import BINANCE_FUTURES, CANDLES, FUNDING, L2_BOOK, LIQUIDATIONS, PERPETUAL, TICKER, TRADES
from cryptofeed.exchanges import Binance
from cryptofeed.exchanges.binance_futures import BinanceFutures
from cryptofeed.symbols import Symbols


@pytest.mark.xfail(reason="Binance blocks build machine IP ranges. If outside the USA this should pass")
def test_binance_address_generation():
    symbols = Binance.symbols()
    channels = [channel for channel in Binance.info()['channels']['websocket'] if not Binance.is_authenticated_channel(channel)]
    for length in (10, 20, 30, 40, 50, 100, 200, 500, len(symbols)):
        syms = []
        chans = []

        sub = random.sample(symbols, length)
        addr = Binance(symbols=sub, channels=channels)._address()

        if length * len(channels) < 1024:
            assert isinstance(addr, str)
            value = addr.split("=", 1)[1]
            value = value.split("/")
            for entry in value:
                sym, chan = entry.split("@", 1)
                syms.append(sym)
                chans.append(chan)
        else:
            assert isinstance(addr, list)

            for value in addr:
                value = value.split("=", 1)[1]
                value = value.split("/")
                for entry in value:
                    sym, chan = entry.split("@", 1)
                    syms.append(sym)
                    chans.append(chan)
        assert len(chans) == len(channels) * length == len(syms)
        assert len(set(chans)) == len(channels)
        assert (len(set(syms))) == length


@pytest.fixture
def binance_futures_symbols():
    old_data = Symbols.data.copy()
    Symbols.set(BINANCE_FUTURES, {
        'BTC-USDT-PERP': 'BTCUSDT',
        'BTC-USDT-PINDEX': 'pBTCUSDT',
        'XAU-USDT-PERP': 'XAUUSDT',
        'XAU-USDT-PINDEX': 'pXAUUSDT',
    }, {
        'instrument_type': {
            'BTC-USDT-PERP': PERPETUAL,
            'XAU-USDT-PERP': PERPETUAL,
        }
    })
    yield
    Symbols.data = old_data


def test_binance_futures_address_generation_routes_funding_and_candles_to_market(binance_futures_symbols):
    addr = BinanceFutures(symbols=['BTC-USDT-PERP'], channels=[FUNDING])._address()
    assert addr == 'wss://fstream.binance.com/market/stream?streams=btcusdt@markPrice'

    addr = BinanceFutures(symbols=['BTC-USDT-PERP'], channels=[CANDLES])._address()
    assert addr == 'wss://fstream.binance.com/market/stream?streams=btcusdt@kline_1m'


def test_binance_futures_address_generation_splits_public_and_market(binance_futures_symbols):
    addr = BinanceFutures(symbols=['BTC-USDT-PERP'], channels=[L2_BOOK, TRADES, FUNDING, CANDLES, TICKER, LIQUIDATIONS])._address()

    assert len(addr) == 2
    public = next(url for url in addr if '/public/' in url)
    market = next(url for url in addr if '/market/' in url)

    assert set(public.split('=', 1)[1].split('/')) == {'btcusdt@bookTicker', 'btcusdt@depth@100ms'}
    assert set(market.split('=', 1)[1].split('/')) == {
        'btcusdt@aggTrade',
        'btcusdt@kline_1m',
        'btcusdt@forceOrder',
        'btcusdt@markPrice',
    }


def test_binance_futures_parse_tradifi_perpetual_as_perp_and_pindex():
    symbols, info = BinanceFutures._parse_symbol_data({
        'symbols': [
            {
                'symbol': 'XAUUSDT',
                'status': 'TRADING',
                'contractStatus': 'TRADING',
                'contractType': 'TRADIFI_PERPETUAL',
                'baseAsset': 'XAU',
                'quoteAsset': 'USDT',
                'filters': [{'tickSize': '0.01'}],
            },
        ],
    })

    assert symbols['XAU-USDT-PERP'] == 'XAUUSDT'
    assert symbols['XAU-USDT-PINDEX'] == 'pXAUUSDT'
    assert info['instrument_type']['XAU-USDT-PERP'] == PERPETUAL
