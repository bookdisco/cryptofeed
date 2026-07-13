'''
Copyright (C) 2017-2025 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
from types import SimpleNamespace

import pytest
from websockets.protocol import State
from yapic import json

from cryptofeed.connection import WSAsyncConn
from cryptofeed.defines import BINANCE, BINANCE_FUTURES, L2_BOOK, PERPETUAL, TRADES
from cryptofeed.exchanges.binance import Binance
from cryptofeed.exchanges.binance_futures import BinanceFutures
from cryptofeed.symbols import Symbols


class DummySocket:
    def __init__(self, feed=None, connection=None, frames=None, error=None):
        self.feed = feed
        self.connection = connection
        self.frames = frames if frames is not None else []
        self.error = error
        self.state = State.OPEN

    async def send(self, payload):
        frame = json.loads(payload)
        self.frames.append(frame)
        if self.feed:
            response = ({'code': self.error[0], 'msg': self.error[1], 'id': frame['id']}
                        if self.error else {'result': None, 'id': frame['id']})
            await self.feed.control_message_handler(json.dumps(response), self.connection)

    async def close(self):
        self.state = State.CLOSED


@pytest.fixture
def binance_symbols():
    old_data = Symbols.data.copy()
    Symbols.set(BINANCE, {
        'BTC-USDT': 'BTCUSDT',
        'ETH-USDT': 'ETHUSDT',
        'SOL-USDT': 'SOLUSDT',
    }, {'instrument_type': {}})
    yield
    Symbols.data = old_data


@pytest.fixture
def binance_futures_symbols():
    old_data = Symbols.data.copy()
    Symbols.set(BINANCE_FUTURES, {
        'BTC-USDT-PERP': 'BTCUSDT',
        'ETH-USDT-PERP': 'ETHUSDT',
    }, {'instrument_type': {
        'BTC-USDT-PERP': PERPETUAL,
        'ETH-USDT-PERP': PERPETUAL,
    }})
    yield
    Symbols.data = old_data


def running_feed(feed):
    feed._loop = asyncio.get_running_loop()
    feed._running = True
    return feed


def add_live_connection(feed, address):
    frames = []
    connection = WSAsyncConn(address, feed.id)
    connection.conn = DummySocket(feed, connection, frames)
    feed.connection_handlers.append(SimpleNamespace(conn=connection))
    return connection, frames


@pytest.mark.asyncio
async def test_capacity_placement_and_dynamic_connection_spawn(monkeypatch, binance_symbols):
    feed = running_feed(Binance(symbols=['BTC-USDT'], channels=[L2_BOOK], subscription_headroom=1))
    feed.per_connection_limit = 3
    connection, frames = add_live_connection(
        feed, 'wss://stream.binance.com:9443/stream?streams=btcusdt@depth@100ms')
    spawned = []

    def fake_start(conn, subscribe, handler, authenticate, loop):
        connection_handler = SimpleNamespace(conn=conn)
        feed.connection_handlers.append(connection_handler)
        spawned.append(connection_handler)
        return connection_handler

    monkeypatch.setattr(feed, '_start_connection', fake_start)

    await feed.add_symbols(['ETH-USDT'])
    assert frames[0]['method'] == 'SUBSCRIBE'
    assert frames[0]['params'] == ['ethusdt@depth@100ms']
    assert len(connection.streams) == 2

    await feed.add_symbols(['SOL-USDT'])
    assert len(spawned) == 1
    assert spawned[0].conn.streams == ['solusdt@depth@100ms']
    assert spawned[0].conn.address.startswith('wss://stream.binance.com:9443/stream?streams=')
    await connection.close()


@pytest.mark.asyncio
async def test_frame_batching_and_spot_rate_pacing(monkeypatch, binance_symbols):
    feed = running_feed(Binance(symbols=['BTC-USDT'], channels=[L2_BOOK, TRADES]))
    connection, frames = add_live_connection(
        feed,
        'wss://stream.binance.com:9443/stream?streams='
        'btcusdt@depth@100ms/btcusdt@aggTrade')

    await feed.add_symbols(['ETH-USDT'])
    assert frames[0]['method'] == 'SUBSCRIBE'
    assert set(frames[0]['params']) == {'ethusdt@depth@100ms', 'ethusdt@aggTrade'}

    sleeps = []

    async def paced_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr('cryptofeed.exchanges.binance.asyncio.sleep', paced_sleep)
    await feed.remove_symbols(['ETH-USDT'])

    assert frames[1]['method'] == 'UNSUBSCRIBE'
    assert set(frames[1]['params']) == {'ethusdt@depth@100ms', 'ethusdt@aggTrade'}
    assert sleeps and sleeps[0] > 0.19
    assert connection.streams == ['btcusdt@depth@100ms', 'btcusdt@aggTrade']
    await connection.close()


@pytest.mark.asyncio
async def test_reconnect_uses_added_streams_and_omits_removed_streams(monkeypatch, binance_symbols):
    feed = running_feed(Binance(symbols=['BTC-USDT'], channels=[L2_BOOK]))
    connection, _ = add_live_connection(
        feed, 'wss://stream.binance.com:9443/stream?streams=btcusdt@depth@100ms')

    await feed.add_symbols(['ETH-USDT'])
    await feed.remove_symbols(['BTC-USDT'])
    await connection.close()

    dialed = []

    async def reconnect(address, **kwargs):
        dialed.append(address)
        return DummySocket()

    monkeypatch.setattr('cryptofeed.connection.connect', reconnect)
    await connection._open()
    await feed.subscribe(connection)

    assert dialed == ['wss://stream.binance.com:9443/stream?streams=ethusdt@depth@100ms']
    assert 'btcusdt' not in connection.address
    await connection.close()


@pytest.mark.asyncio
async def test_unknown_symbol_mapping_is_injected_without_replacing_existing(binance_symbols):
    feed = running_feed(Binance(symbols=['BTC-USDT'], channels=[L2_BOOK]))
    connection, frames = add_live_connection(
        feed, 'wss://stream.binance.com:9443/stream?streams=btcusdt@depth@100ms')

    async def exchange_info(address):
        return json.dumps({'symbols': [{
            'symbol': 'XRPUSDT',
            'status': 'TRADING',
            'baseAsset': 'XRP',
            'quoteAsset': 'USDT',
            'filters': [{'tickSize': '0.0001'}],
        }]})

    feed.http_conn.read = exchange_info
    await feed.add_symbols(['XRP-USDT'])

    assert feed.normalized_symbol_mapping['BTC-USDT'] == 'BTCUSDT'
    assert feed.normalized_symbol_mapping['XRP-USDT'] == 'XRPUSDT'
    assert feed.exchange_symbol_mapping['XRPUSDT'] == 'XRP-USDT'
    assert frames[0]['params'] == ['xrpusdt@depth@100ms']
    await connection.close()


@pytest.mark.asyncio
async def test_remove_clears_symbol_state_and_frees_stream_slots(binance_symbols):
    feed = running_feed(Binance(symbols=['BTC-USDT', 'ETH-USDT'], channels=[L2_BOOK]))
    connection, frames = add_live_connection(
        feed,
        'wss://stream.binance.com:9443/stream?streams='
        'btcusdt@depth@100ms/ethusdt@depth@100ms')
    feed._l2_book['ETH-USDT'] = object()
    feed._l3_book['ETH-USDT'] = object()
    feed.last_update_id['ETH-USDT'] = 10
    feed.previous_book['ETH-USDT'] = object()
    feed._sequence_no['ETH-USDT'] = 10
    feed._open_interest_cache['ETHUSDT'] = '1'

    await feed.remove_symbols(['ETH-USDT'])

    assert frames[0]['method'] == 'UNSUBSCRIBE'
    assert connection.streams == ['btcusdt@depth@100ms']
    assert 'ETHUSDT' not in feed.subscription['depth']
    assert 'ETH-USDT' not in feed._l2_book
    assert 'ETH-USDT' not in feed._l3_book
    assert 'ETH-USDT' not in feed.last_update_id
    assert 'ETH-USDT' not in feed.previous_book
    assert 'ETH-USDT' not in feed._sequence_no
    assert 'ETHUSDT' not in feed._open_interest_cache
    await connection.close()


@pytest.mark.asyncio
async def test_dynamic_routing_uses_subclass_address_override(binance_futures_symbols):
    class TradeOnPublicBinanceFutures(BinanceFutures):
        websocket_channels = {**BinanceFutures.websocket_channels, TRADES: 'trade'}

        def _address(self):
            address = super()._address()
            addresses = address if isinstance(address, list) else [address]
            routed = [item.replace('/market/', '/public/') if '@trade' in item else item for item in addresses]
            return routed if isinstance(address, list) else routed[0]

    feed = running_feed(TradeOnPublicBinanceFutures(
        symbols=['BTC-USDT-PERP'], channels=[L2_BOOK, TRADES]))
    connection, frames = add_live_connection(
        feed,
        'wss://fstream.binance.com/public/stream?streams='
        'btcusdt@depth@100ms/btcusdt@trade')

    await feed.add_symbols(['ETH-USDT-PERP'])

    assert len(frames) == 1
    assert set(frames[0]['params']) == {'ethusdt@depth@100ms', 'ethusdt@trade'}
    await connection.close()


@pytest.mark.asyncio
async def test_control_error_is_logged_and_raised_without_persisting_stream(caplog, binance_symbols):
    feed = running_feed(Binance(symbols=['BTC-USDT'], channels=[L2_BOOK]))
    connection, _ = add_live_connection(
        feed, 'wss://stream.binance.com:9443/stream?streams=btcusdt@depth@100ms')
    connection.conn.error = (2, 'Invalid request')

    with pytest.raises(RuntimeError, match='Invalid request'):
        await feed.add_symbols(['ETH-USDT'])

    assert connection.streams == ['btcusdt@depth@100ms']
    assert 'ETHUSDT' not in feed.subscription['depth']
    assert 'subscription command failed' in caplog.text
    await connection.close()
