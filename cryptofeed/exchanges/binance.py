'''
Copyright (C) 2017-2025 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
import logging
from asyncio import create_task, sleep
from collections import defaultdict
from decimal import Decimal
import requests
import time
from typing import Dict, List, Union, Tuple
from urllib.parse import urlencode

from yapic import json

from cryptofeed.connection import AsyncConnection, HTTPPoll, HTTPConcurrentPoll, RestEndpoint, Routes, WebsocketEndpoint, WSAsyncConn
from cryptofeed.defines import ASK, BALANCES, BID, BINANCE, BUY, CANDLES, FUNDING, FUTURES, L2_BOOK, LIMIT, LIQUIDATIONS, MARKET, OPEN_INTEREST, ORDER_INFO, PERPETUAL, SELL, SPOT, TICKER, TRADES, FILLED, UNFILLED
from cryptofeed.feed import Feed
from cryptofeed.exceptions import UnsupportedSymbol
from cryptofeed.symbols import Symbol, Symbols
from cryptofeed.exchanges.mixins.binance_rest import BinanceRestMixin
from cryptofeed.types import Trade, Ticker, Candle, Liquidation, Funding, OrderBook, OrderInfo, Balance

REFRESH_SNAPSHOT_MIN_INTERVAL_SECONDS = 60

LOG = logging.getLogger('feedhandler')


class Binance(Feed, BinanceRestMixin):
    id = BINANCE
    websocket_endpoints = [WebsocketEndpoint('wss://stream.binance.com:9443', sandbox='wss://testnet.binance.vision')]
    rest_endpoints = [RestEndpoint('https://api.binance.com', routes=Routes('/api/v3/exchangeInfo', l2book='/api/v3/depth?symbol={}&limit={}', authentication='/api/v3/userDataStream'), sandbox='https://testnet.binance.vision')]

    valid_depths = [5, 10, 20, 50, 100, 500, 1000, 5000]
    # m -> minutes; h -> hours; d -> days; w -> weeks; M -> months
    valid_candle_intervals = {'1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M'}
    valid_depth_intervals = {'100ms', '1000ms'}
    websocket_channels = {
        L2_BOOK: 'depth',
        TRADES: 'aggTrade',
        TICKER: 'bookTicker',
        CANDLES: 'kline_',
        BALANCES: BALANCES,
        ORDER_INFO: ORDER_INFO
    }
    request_limit = 20
    per_connection_limit = 1024
    ws_control_message_rate = 5

    @classmethod
    def timestamp_normalize(cls, ts: float) -> float:
        return ts / 1000.0

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)
        for symbol in data['symbols']:
            if symbol.get('status', 'TRADING') != "TRADING":
                continue
            if symbol.get('contractStatus', 'TRADING') != "TRADING":
                continue

            expiration = None
            stype = SPOT
            if symbol.get('contractType') in ('PERPETUAL', 'TRADIFI_PERPETUAL'):
                stype = PERPETUAL
            elif symbol.get('contractType') in ('CURRENT_QUARTER', 'NEXT_QUARTER'):
                stype = FUTURES
                expiration = symbol['symbol'].split("_")[1]

            s = Symbol(symbol['baseAsset'], symbol['quoteAsset'], type=stype, expiry_date=expiration)
            ret[s.normalized] = symbol['symbol']
            info['tick_size'][s.normalized] = symbol['filters'][0]['tickSize']
            info['instrument_type'][s.normalized] = stype
        return ret, info

    def __init__(self, depth_interval='100ms', subscription_headroom=24, **kwargs):
        """
        depth_interval: str
            time between l2_book/delta updates {'100ms', '1000ms'} (different from BINANCE_FUTURES & BINANCE_DELIVERY)
        subscription_headroom: int
            stream slots reserved on each connection for operational headroom
        """
        if depth_interval is not None and depth_interval not in self.valid_depth_intervals:
            raise ValueError(f"Depth interval must be one of {self.valid_depth_intervals}")
        if subscription_headroom < 0 or subscription_headroom >= self.per_connection_limit:
            raise ValueError("subscription_headroom must be between 0 and per_connection_limit")

        super().__init__(**kwargs)
        self.depth_interval = depth_interval
        self.subscription_headroom = subscription_headroom
        self._dynamic_subscription_lock = asyncio.Lock()
        self._control_message_lock = asyncio.Lock()
        self._last_control_message = 0.0
        self._control_message_id = 0
        self._pending_control_messages = {}
        self._control_ack_timeout = 10.0
        self._open_interest_cache = {}
        self._reset()

    def _address(self) -> Union[str, Dict]:
        """
        Binance has a 200 pair/stream limit per connection, so we need to break the address
        down into multiple connections if necessary. Because the key is currently not used
        for the address dict, we can just set it to the last used stream, since this will be
        unique.

        The generic connect method supplied by Feed will take care of creating the
        correct connection objects from the addresses.
        """
        if self.requires_authentication:
            listen_key = self._generate_token()
            address = self.address
            address += '/ws/' + listen_key
        else:
            address = self.address
            address += '/stream?streams='
        subs = []

        is_any_private = any(self.is_authenticated_channel(chan) for chan in self.subscription)
        is_any_public = any(not self.is_authenticated_channel(chan) for chan in self.subscription)
        if is_any_private and is_any_public:
            raise ValueError("Private channels should be subscribed in separate feeds vs public channels")
        if all(self.is_authenticated_channel(chan) for chan in self.subscription):
            return address

        for chan in self.subscription:
            normalized_chan = self.exchange_channel_to_std(chan)
            if normalized_chan == OPEN_INTEREST:
                continue
            if self.is_authenticated_channel(normalized_chan):
                continue

            stream = chan
            if normalized_chan == CANDLES:
                stream = f"{chan}{self.candle_interval}"
            elif normalized_chan == L2_BOOK:
                stream = f"{chan}@{self.depth_interval}"

            for pair in self.subscription[chan]:
                # for everything but premium index the symbols need to be lowercase.
                if pair.startswith("p"):
                    if normalized_chan != CANDLES:
                        raise ValueError("Premium Index Symbols only allowed on Candle data feed")
                else:
                    pair = pair.lower()
                subs.append(f"{pair}@{stream}")

        if 0 < len(subs) < self.per_connection_limit:
            return address + '/'.join(subs)
        else:
            def split_list(_list: list, n: int):
                for i in range(0, len(_list), n):
                    yield _list[i:i + n]

            return [address + '/'.join(chunk) for chunk in split_list(subs, self.per_connection_limit)]

    def _assert_dynamic_subscription_loop(self):
        """Dynamic subscription mutations are intentionally single-event-loop only."""
        loop = asyncio.get_running_loop()
        if self._loop is None or not self._running:
            raise RuntimeError('The feed must be started before symbols can be changed')
        if loop is not self._loop:
            raise RuntimeError('add_symbols/remove_symbols must run on the feed event loop')

    @staticmethod
    def _normalized_dynamic_symbols(symbols) -> List[str]:
        ret = []
        for symbol in symbols:
            normalized = symbol.normalized if isinstance(symbol, Symbol) else symbol
            if normalized not in ret:
                ret.append(normalized)
        return ret

    async def _refresh_unknown_symbol_mappings(self, symbols: List[str]):
        """Fetch current exchange metadata and inject only newly requested symbols."""
        unknown = [symbol for symbol in symbols if symbol not in self.normalized_symbol_mapping]
        if not unknown:
            return

        data = []
        for endpoint in self.rest_endpoints:
            address = endpoint.route('instruments', sandbox=self.sandbox)
            addresses = address if isinstance(address, list) else [address]
            for address in addresses:
                LOG.info('%s: refreshing symbol information from %s', self.id, address)
                response = await self.http_conn.read(address)
                data.append(json.loads(response) if isinstance(response, (str, bytes)) else response)

        fresh_mapping, fresh_info = type(self)._parse_symbol_data(data if len(data) > 1 else data[0])
        missing = [symbol for symbol in unknown if symbol not in fresh_mapping]
        if missing:
            raise UnsupportedSymbol(f'{", ".join(missing)} is not supported on {self.id}')

        merged_mapping = dict(self.normalized_symbol_mapping)
        try:
            _, current_info = Symbols.get(self.id)
        except KeyError:
            current_info = {}
        merged_info = {key: dict(value) if isinstance(value, dict) else value for key, value in current_info.items()}

        for symbol in unknown:
            merged_mapping[symbol] = fresh_mapping[symbol]
            for key, values in fresh_info.items():
                if isinstance(values, dict) and symbol in values:
                    merged_info.setdefault(key, {})[symbol] = values[symbol]

        # Preserve every startup entry even if a later exchangeInfo response omits it.
        Symbols.set(self.id, merged_mapping, merged_info)
        self.normalized_symbol_mapping = merged_mapping
        self.exchange_symbol_mapping = {value: key for key, value in merged_mapping.items()}

    def _dynamic_stream_specs(self, exchange_symbol: str):
        """
        Ask the active ``_address`` override to route every new stream.

        The one-stream call is deliberate: downstream subclasses can change Binance's
        path rules (for example, routing ``trade`` to ``/public``), and their override
        remains the single source of truth for both startup and runtime placement.
        """
        specs = []
        original_subscription = self.subscription
        try:
            for channel in original_subscription:
                normalized_channel = self.exchange_channel_to_std(channel)
                if normalized_channel == OPEN_INTEREST or self.is_authenticated_channel(normalized_channel):
                    continue
                if exchange_symbol in original_subscription[channel]:
                    continue

                self.subscription = {channel: [exchange_symbol]}
                address = self._address()
                if isinstance(address, list):
                    if len(address) != 1:
                        raise RuntimeError(f'Expected one address for one Binance stream, got {address!r}')
                    address = address[0]
                prefix, separator, stream = address.partition('streams=')
                if not separator or not stream or '/' in stream:
                    raise RuntimeError(f'Unable to derive Binance stream route from {address!r}')
                specs.append({'channel': channel, 'exchange_symbol': exchange_symbol, 'stream': stream, 'prefix': prefix})
        finally:
            self.subscription = original_subscription
        return specs

    @staticmethod
    def _connection_streams(connection) -> List[str]:
        return list(connection.streams)

    @staticmethod
    def _set_connection_streams(connection, streams: List[str]):
        connection.set_streams(streams)

    async def _send_control_message(self, connection, method: str, streams: List[str]):
        loop = asyncio.get_running_loop()
        self._control_message_id += 1
        message_id = self._control_message_id
        future = loop.create_future()
        self._pending_control_messages[message_id] = future
        frame = {'method': method, 'params': streams, 'id': message_id}

        try:
            async with self._control_message_lock:
                interval = 1.0 / self.ws_control_message_rate
                delay = interval - (loop.time() - self._last_control_message)
                if delay > 0:
                    await asyncio.sleep(delay)
                await connection.write(json.dumps(frame))
                self._last_control_message = loop.time()
            await asyncio.wait_for(future, timeout=self._control_ack_timeout)
        finally:
            self._pending_control_messages.pop(message_id, None)

    async def control_message_handler(self, message: str, connection: AsyncConnection) -> bool:
        """Consume Binance command acknowledgements before exchange event dispatch."""
        if isinstance(message, str) and '"id"' not in message and '"code"' not in message:
            return False
        try:
            payload = json.loads(message)
        except (TypeError, ValueError):
            return False
        if not isinstance(payload, dict) or ('result' not in payload and 'code' not in payload):
            return False

        message_id = payload.get('id')
        future = self._pending_control_messages.get(message_id)
        if 'code' in payload:
            error = RuntimeError(f"{self.id} subscription command failed: {payload.get('code')} {payload.get('msg', '')}".rstrip())
            LOG.error('%s', error)
            if future and not future.done():
                future.set_exception(error)
            elif message_id is None:
                for pending in self._pending_control_messages.values():
                    if not pending.done():
                        pending.set_exception(error)
            return True

        if payload.get('result') is None:
            if future and not future.done():
                future.set_result(payload)
            else:
                LOG.debug('%s: received acknowledgement for unknown command id %s', self.id, message_id)
            return True
        return False

    def _spawn_dynamic_connection(self, specs):
        streams = [spec['stream'] for spec in specs]
        address = specs[0]['prefix'] + 'streams=' + '/'.join(streams)
        subscription = defaultdict(list)
        for spec in specs:
            subscription[spec['channel']].append(spec['exchange_symbol'])
        endpoint = self.websocket_endpoints[0]
        connection = WSAsyncConn(address, self.id, subscription=dict(subscription), **endpoint.options)
        return self._start_connection(connection, self.subscribe, self.message_handler, self.authenticate, self._loop)

    def _commit_added_symbols(self, normalized_symbols: List[str]):
        for normalized_symbol in normalized_symbols:
            exchange_symbol = self.normalized_symbol_mapping[normalized_symbol]
            for channel, subscribed in self.subscription.items():
                if exchange_symbol not in subscribed:
                    if hasattr(subscribed, 'add'):
                        subscribed.add(exchange_symbol)
                    else:
                        subscribed.append(exchange_symbol)
                standard_channel = self.exchange_channel_to_std(channel)
                configured = self._feed_config.setdefault(standard_channel, [])
                if normalized_symbol not in configured:
                    configured.append(normalized_symbol)
            if normalized_symbol not in self.normalized_symbols:
                self.normalized_symbols.append(normalized_symbol)

    def _commit_removed_symbols(self, normalized_symbols: List[str]):
        for normalized_symbol in normalized_symbols:
            exchange_symbol = self.normalized_symbol_mapping.get(normalized_symbol)
            if exchange_symbol is None:
                continue
            for channel, subscribed in self.subscription.items():
                while exchange_symbol in subscribed:
                    subscribed.remove(exchange_symbol)
                standard_channel = self.exchange_channel_to_std(channel)
                configured = self._feed_config.get(standard_channel, [])
                while normalized_symbol in configured:
                    configured.remove(normalized_symbol)
            while normalized_symbol in self.normalized_symbols:
                self.normalized_symbols.remove(normalized_symbol)

    def _sync_open_interest_poll_addresses(self):
        """Keep Binance Futures' REST-polled open-interest channel in lockstep."""
        channel = self.websocket_channels.get(OPEN_INTEREST)
        if channel not in self.subscription:
            return
        addresses = [self.rest_endpoints[0].route('open_interest', sandbox=self.sandbox).format(symbol)
                     for symbol in self.subscription[channel]]
        for handler in self.connection_handlers:
            if isinstance(handler.conn, HTTPPoll):
                handler.conn.address = addresses

    def _clear_symbol_state(self, normalized_symbol: str, exchange_symbol: str):
        """Discard state so a later L2 diff follows the normal snapshot bootstrap."""
        self._l2_book.pop(normalized_symbol, None)
        self._l3_book.pop(normalized_symbol, None)
        self.last_update_id.pop(normalized_symbol, None)
        self.previous_book.pop(normalized_symbol, None)
        self._sequence_no.pop(normalized_symbol, None)
        self._open_interest_cache.pop(exchange_symbol, None)
        self._open_interest_cache.pop(normalized_symbol, None)

    async def add_symbols(self, symbols):
        """
        Subscribe symbols on a running Binance feed using its existing channels.

        This coroutine must be called on the event loop that started the feed.
        """
        self._assert_dynamic_subscription_loop()
        if self.requires_authentication:
            raise NotImplementedError('Runtime symbol changes are only supported for public Binance feeds')
        normalized_symbols = self._normalized_dynamic_symbols(symbols)

        async with self._dynamic_subscription_lock:
            await self._refresh_unknown_symbol_mappings(normalized_symbols)
            specs = []
            for normalized_symbol in normalized_symbols:
                exchange_symbol = self.normalized_symbol_mapping[normalized_symbol]
                symbol_specs = self._dynamic_stream_specs(exchange_symbol)
                if symbol_specs:
                    self._clear_symbol_state(normalized_symbol, exchange_symbol)
                    specs.extend(symbol_specs)
            if not specs:
                self._commit_added_symbols(normalized_symbols)
                self._sync_open_interest_poll_addresses()
                return

            capacity = self.per_connection_limit - self.subscription_headroom
            live_connections = [handler.conn for handler in self.connection_handlers if isinstance(handler.conn, WSAsyncConn) and handler.conn.is_open]
            assignments = defaultdict(list)
            counts = {connection: len(self._connection_streams(connection)) for connection in live_connections}
            unplaced = defaultdict(list)

            for spec in specs:
                candidates = [connection for connection in live_connections
                              if connection.stream_address_prefix == spec['prefix'] and counts[connection] < capacity]
                if candidates:
                    connection = min(candidates, key=lambda item: counts[item])
                    assignments[connection].append(spec)
                    counts[connection] += 1
                else:
                    unplaced[spec['prefix']].append(spec)

            successful = []
            try:
                for connection, assigned in assignments.items():
                    streams = [spec['stream'] for spec in assigned]
                    await self._send_control_message(connection, 'SUBSCRIBE', streams)
                    successful.append((connection, streams))
            except Exception:
                for connection, streams in successful:
                    try:
                        await self._send_control_message(connection, 'UNSUBSCRIBE', streams)
                    except Exception:
                        LOG.error('%s: failed to roll back a partial runtime subscription', self.id, exc_info=True)
                raise

            for connection, streams in successful:
                self._set_connection_streams(connection, self._connection_streams(connection) + streams)

            for pending in unplaced.values():
                for offset in range(0, len(pending), capacity):
                    self._spawn_dynamic_connection(pending[offset:offset + capacity])

            self._commit_added_symbols(normalized_symbols)
            self._sync_open_interest_poll_addresses()

    async def remove_symbols(self, symbols):
        """
        Unsubscribe symbols and clear their local state on a running Binance feed.

        This coroutine must be called on the event loop that started the feed.
        Empty connections deliberately remain under their existing handler lifecycle.
        """
        self._assert_dynamic_subscription_loop()
        normalized_symbols = self._normalized_dynamic_symbols(symbols)

        async with self._dynamic_subscription_lock:
            requested_streams = set()
            exchange_symbols = {}
            for normalized_symbol in normalized_symbols:
                exchange_symbol = self.normalized_symbol_mapping.get(normalized_symbol)
                if exchange_symbol is None:
                    continue
                exchange_symbols[normalized_symbol] = exchange_symbol
                original_subscription = self.subscription
                try:
                    # Build the same stream names while they are still in the registry.
                    for channel in original_subscription:
                        normalized_channel = self.exchange_channel_to_std(channel)
                        if normalized_channel == OPEN_INTEREST or self.is_authenticated_channel(normalized_channel):
                            continue
                        self.subscription = {channel: []}
                        self.subscription[channel] = [exchange_symbol]
                        address = self._address()
                        if isinstance(address, list):
                            address = address[0]
                        _, separator, stream = address.partition('streams=')
                        if separator and stream:
                            requested_streams.add(stream)
                finally:
                    self.subscription = original_subscription

            removals = []
            for handler in self.connection_handlers:
                connection = handler.conn
                if not isinstance(connection, WSAsyncConn):
                    continue
                present = [stream for stream in self._connection_streams(connection) if stream in requested_streams]
                if not present:
                    continue
                removals.append((connection, present))

            successful = []
            try:
                for connection, present in removals:
                    if connection.is_open:
                        await self._send_control_message(connection, 'UNSUBSCRIBE', present)
                        successful.append((connection, present))
            except Exception:
                for connection, present in successful:
                    try:
                        await self._send_control_message(connection, 'SUBSCRIBE', present)
                    except Exception:
                        LOG.error('%s: failed to roll back a partial runtime unsubscription', self.id, exc_info=True)
                raise

            for connection, present in removals:
                remaining = [stream for stream in self._connection_streams(connection) if stream not in requested_streams]
                self._set_connection_streams(connection, remaining)

            self._commit_removed_symbols(normalized_symbols)
            self._sync_open_interest_poll_addresses()
            for normalized_symbol, exchange_symbol in exchange_symbols.items():
                self._clear_symbol_state(normalized_symbol, exchange_symbol)

    def _reset(self):
        self._l2_book = {}
        self.last_update_id = {}

    async def _refresh_token(self):
        while True:
            await sleep(30 * 60)
            if self._auth_token is None:
                raise ValueError('There is no token to refresh')
            payload = {'listenKey': self._auth_token}
            r = requests.put(f'{self.rest_endpoints[0].route("authentication", sandbox=self.sandbox)}?{urlencode(payload)}', headers={'X-MBX-APIKEY': self.key_id})
            r.raise_for_status()

    def _generate_token(self) -> str:
        url = self.rest_endpoints[0].route('authentication', sandbox=self.sandbox)
        r = requests.post(url, headers={'X-MBX-APIKEY': self.key_id})
        r.raise_for_status()
        response = r.json()
        if 'listenKey' in response:
            self._auth_token = response['listenKey']
            return self._auth_token
        else:
            raise ValueError(f'Unable to retrieve listenKey token from {url}')

    async def _trade(self, msg: dict, timestamp: float):
        """
        {
            "e": "aggTrade",  // Event type
            "E": 123456789,   // Event time
            "s": "BNBBTC",    // Symbol
            "a": 12345,       // Aggregate trade ID
            "p": "0.001",     // Price
            "q": "100",       // Quantity
            "f": 100,         // First trade ID
            "l": 105,         // Last trade ID
            "T": 123456785,   // Trade time
            "m": true,        // Is the buyer the market maker?
            "M": true         // Ignore
        }
        """
        t = Trade(self.id,
                  self.exchange_symbol_to_std_symbol(msg['s']),
                  SELL if msg['m'] else BUY,
                  Decimal(msg['q']),
                  Decimal(msg['p']),
                  self.timestamp_normalize(msg['T']),
                  id=str(msg['a']),
                  raw=msg)
        await self.callback(TRADES, t, timestamp)

    async def _ticker(self, msg: dict, timestamp: float):
        """
        {
            'u': 382569232,
            's': 'FETUSDT',
            'b': '0.36031000',
            'B': '1500.00000000',
            'a': '0.36092000',
            'A': '176.40000000'
        }
        """
        pair = self.exchange_symbol_to_std_symbol(msg['s'])
        bid = Decimal(msg['b'])
        ask = Decimal(msg['a'])

        # Binance does not have a timestamp in this update, but the two futures APIs do
        if 'E' in msg:
            ts = self.timestamp_normalize(msg['E'])
        else:
            ts = timestamp

        t = Ticker(self.id, pair, bid, ask, ts, raw=msg)
        await self.callback(TICKER, t, timestamp)

    async def _liquidations(self, msg: dict, timestamp: float):
        """
        {
        "e":"forceOrder",       // Event Type
        "E":1568014460893,      // Event Time
        "o":{
            "s":"BTCUSDT",      // Symbol
            "S":"SELL",         // Side
            "o":"LIMIT",        // Order Type
            "f":"IOC",          // Time in Force
            "q":"0.014",        // Original Quantity
            "p":"9910",         // Price
            "ap":"9910",        // Average Price
            "X":"FILLED",       // Order Status
            "l":"0.014",        // Order Last Filled Quantity
            "z":"0.014",        // Order Filled Accumulated Quantity
            "T":1568014460893,  // Order Trade Time
            }
        }
        """
        pair = self.exchange_symbol_to_std_symbol(msg['o']['s'])
        liq = Liquidation(self.id,
                          pair,
                          SELL if msg['o']['S'] == 'SELL' else BUY,
                          Decimal(msg['o']['q']),
                          Decimal(msg['o']['p']),
                          None,
                          FILLED if msg['o']['X'] == 'FILLED' else UNFILLED,
                          self.timestamp_normalize(msg['E']),
                          raw=msg)
        await self.callback(LIQUIDATIONS, liq, receipt_timestamp=timestamp)

    def _check_update_id(self, std_pair: str, msg: dict) -> bool:
        """
        Messages will be queued while fetching snapshot and we can return a book_callback
        using this msg's data instead of waiting for the next update.
        """
        if self._l2_book[std_pair].delta is None and msg['u'] <= self.last_update_id[std_pair]:
            return True
        elif msg['U'] <= self.last_update_id[std_pair] and msg['u'] <= self.last_update_id[std_pair]:
            # Old message, can ignore it
            return True
        elif msg['U'] <= self.last_update_id[std_pair] + 1 <= msg['u']:
            self.last_update_id[std_pair] = msg['u']
            return False
        elif self.last_update_id[std_pair] + 1 == msg['U']:
            self.last_update_id[std_pair] = msg['u']
            return False
        else:
            self._reset()
            LOG.warning("%s: Missing book update detected, resetting book", self.id)
            return True

    async def _snapshot(self, pair: str) -> None:
        max_depth = self.max_depth if self.max_depth else 1000
        if max_depth not in self.valid_depths:
            for d in self.valid_depths:
                if d > max_depth:
                    max_depth = d
                    break

        resp = await self.http_conn.read(self.rest_endpoints[0].route('l2book', self.sandbox).format(pair, max_depth))
        resp = json.loads(resp, parse_float=Decimal)
        timestamp = self.timestamp_normalize(resp['E']) if 'E' in resp else None

        std_pair = self.exchange_symbol_to_std_symbol(pair)
        self.last_update_id[std_pair] = resp['lastUpdateId']
        self._l2_book[std_pair] = OrderBook(self.id, std_pair, max_depth=self.max_depth, bids={Decimal(u[0]): Decimal(u[1]) for u in resp['bids']}, asks={Decimal(u[0]): Decimal(u[1]) for u in resp['asks']})
        await self.book_callback(L2_BOOK, self._l2_book[std_pair], time.time(), timestamp=timestamp, raw=resp, sequence_number=self.last_update_id[std_pair])

    async def _book(self, msg: dict, pair: str, timestamp: float):
        """
        {
            "e": "depthUpdate", // Event type
            "E": 123456789,     // Event time
            "s": "BNBBTC",      // Symbol
            "U": 157,           // First update ID in event
            "u": 160,           // Final update ID in event
            "b": [              // Bids to be updated
                    [
                        "0.0024",       // Price level to be updated
                        "10"            // Quantity
                    ]
            ],
            "a": [              // Asks to be updated
                    [
                        "0.0026",       // Price level to be updated
                        "100"           // Quantity
                    ]
            ]
        }
        """
        exchange_pair = pair
        pair = self.exchange_symbol_to_std_symbol(pair)

        if pair not in self._l2_book:
            await self._snapshot(exchange_pair)

        skip_update = self._check_update_id(pair, msg)
        if skip_update:
            return

        delta = {BID: [], ASK: []}

        for s, side in (('b', BID), ('a', ASK)):
            for update in msg[s]:
                price = Decimal(update[0])
                amount = Decimal(update[1])
                delta[side].append((price, amount))

                if amount == 0:
                    if price in self._l2_book[pair].book[side]:
                        del self._l2_book[pair].book[side][price]
                else:
                    self._l2_book[pair].book[side][price] = amount

        await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, timestamp=self.timestamp_normalize(msg['E']), raw=msg, delta=delta, sequence_number=self.last_update_id[pair])

    async def _funding(self, msg: dict, timestamp: float):
        """
        {
            "e": "markPriceUpdate",  // Event type
            "E": 1562305380000,      // Event time
            "s": "BTCUSDT",          // Symbol
            "p": "11185.87786614",   // Mark price
            "r": "0.00030000",       // Funding rate
            "T": 1562306400000       // Next funding time
        }

        BinanceFutures
        {
            "e": "markPriceUpdate",     // Event type
            "E": 1562305380000,         // Event time
            "s": "BTCUSDT",             // Symbol
            "p": "11185.87786614",      // Mark price
            "i": "11784.62659091"       // Index price
            "P": "11784.25641265",      // Estimated Settle Price, only useful in the last hour before the settlement starts
            "r": "0.00030000",          // Funding rate
            "T": 1562306400000          // Next funding time
        }
        """
        next_time = self.timestamp_normalize(msg['T']) if msg['T'] > 0 else None
        rate = Decimal(msg['r']) if msg['r'] else None
        if next_time is None:
            rate = None

        f = Funding(self.id,
                    self.exchange_symbol_to_std_symbol(msg['s']),
                    Decimal(msg['p']),
                    rate,
                    next_time,
                    self.timestamp_normalize(msg['E']),
                    predicted_rate=Decimal(msg['P']) if 'P' in msg and msg['P'] is not None else None,
                    raw=msg)
        await self.callback(FUNDING, f, timestamp)

    async def _candle(self, msg: dict, timestamp: float):
        """
        {
            'e': 'kline',
            'E': 1615927655524,
            's': 'BTCUSDT',
            'k': {
                't': 1615927620000,
                'T': 1615927679999,
                's': 'BTCUSDT',
                'i': '1m',
                'f': 710917276,
                'L': 710917780,
                'o': '56215.99000000',
                'c': '56232.07000000',
                'h': '56238.59000000',
                'l': '56181.99000000',
                'v': '13.80522200',
                'n': 505,
                'x': False,
                'q': '775978.37383076',
                'V': '7.19660600',
                'Q': '404521.60814919',
                'B': '0'
            }
        }
        """
        if self.candle_closed_only and not msg['k']['x']:
            return
        c = Candle(self.id,
                   self.exchange_symbol_to_std_symbol(msg['s']),
                   msg['k']['t'] / 1000,
                   msg['k']['T'] / 1000,
                   msg['k']['i'],
                   msg['k']['n'],
                   Decimal(msg['k']['o']),
                   Decimal(msg['k']['c']),
                   Decimal(msg['k']['h']),
                   Decimal(msg['k']['l']),
                   Decimal(msg['k']['v']),
                   msg['k']['x'],
                   self.timestamp_normalize(msg['E']),
                   raw=msg)
        await self.callback(CANDLES, c, timestamp)

    async def _account_update(self, msg: dict, timestamp: float):
        """
        {
            "e": "outboundAccountPosition", //Event type
            "E": 1564034571105,             //Event Time
            "u": 1564034571073,             //Time of last account update
            "B": [                          //Balances Array
                {
                "a": "ETH",                 //Asset
                "f": "10000.000000",        //Free
                "l": "0.000000"             //Locked
                }
            ]
        }
        """
        for balance in msg['B']:
            b = Balance(
                self.id,
                balance['a'],
                Decimal(balance['f']),
                Decimal(balance['l']),
                raw=msg)
            await self.callback(BALANCES, b, timestamp)

    async def _order_update(self, msg: dict, timestamp: float):
        """
        {
            "e": "executionReport",        // Event type
            "E": 1499405658658,            // Event time
            "s": "ETHBTC",                 // Symbol
            "c": "mUvoqJxFIILMdfAW5iGSOW", // Client order ID
            "S": "BUY",                    // Side
            "o": "LIMIT",                  // Order type
            "f": "GTC",                    // Time in force
            "q": "1.00000000",             // Order quantity
            "p": "0.10264410",             // Order price
            "P": "0.00000000",             // Stop price
            "F": "0.00000000",             // Iceberg quantity
            "g": -1,                       // OrderListId
            "C": "",                       // Original client order ID; This is the ID of the order being canceled
            "x": "NEW",                    // Current execution type
            "X": "NEW",                    // Current order status
            "r": "NONE",                   // Order reject reason; will be an error code.
            "i": 4293153,                  // Order ID
            "l": "0.00000000",             // Last executed quantity
            "z": "0.00000000",             // Cumulative filled quantity
            "L": "0.00000000",             // Last executed price
            "n": "0",                      // Commission amount
            "N": null,                     // Commission asset
            "T": 1499405658657,            // Transaction time
            "t": -1,                       // Trade ID
            "I": 8641984,                  // Ignore
            "w": true,                     // Is the order on the book?
            "m": false,                    // Is this trade the maker side?
            "M": false,                    // Ignore
            "O": 1499405658657,            // Order creation time
            "Z": "0.00000000",             // Cumulative quote asset transacted quantity
            "Y": "0.00000000",             // Last quote asset transacted quantity (i.e. lastPrice * lastQty)
            "Q": "0.00000000"              // Quote Order Qty
        }
        """
        oi = OrderInfo(
            self.id,
            self.exchange_symbol_to_std_symbol(msg['s']),
            str(msg['i']),
            BUY if msg['S'].lower() == 'buy' else SELL,
            msg['x'],
            LIMIT if msg['o'].lower() == 'limit' else MARKET if msg['o'].lower() == 'market' else None,
            Decimal(msg['Z']) / Decimal(msg['z']) if not Decimal.is_zero(Decimal(msg['z'])) else None,
            Decimal(msg['q']),
            Decimal(msg['q']) - Decimal(msg['z']),
            self.timestamp_normalize(msg['E']),
            raw=msg
        )
        await self.callback(ORDER_INFO, oi, timestamp)

    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        # Handle account updates from User Data Stream
        if self.requires_authentication:
            msg_type = msg['e']
            if msg_type == 'outboundAccountPosition':
                await self._account_update(msg, timestamp)
            elif msg_type == 'executionReport':
                await self._order_update(msg, timestamp)
            return
        # Combined stream events are wrapped as follows: {"stream":"<streamName>","data":<rawPayload>}
        # streamName is of format <symbol>@<channel>
        pair, _ = msg['stream'].split('@', 1)
        msg = msg['data']
        pair = pair.upper()
        if 'e' in msg:
            if msg['e'] == 'depthUpdate':
                await self._book(msg, pair, timestamp)
            elif msg['e'] == 'aggTrade':
                await self._trade(msg, timestamp)
            elif msg['e'] == 'forceOrder':
                await self._liquidations(msg, timestamp)
            elif msg['e'] == 'markPriceUpdate':
                await self._funding(msg, timestamp)
            elif msg['e'] == 'kline':
                await self._candle(msg, timestamp)
            else:
                LOG.warning("%s: Unexpected message received: %s", self.id, msg)
        elif 'A' in msg:
            await self._ticker(msg, timestamp)
        else:
            LOG.warning("%s: Unexpected message received: %s", self.id, msg)

    async def subscribe(self, conn: AsyncConnection):
        # Runtime changes are persisted in the URL stream registry, so reconnects
        # bootstrap directly from the durable address without replaying control frames.
        if isinstance(conn, (HTTPPoll, HTTPConcurrentPoll)):
            self._open_interest_cache = {}
        else:
            streams = getattr(conn, 'streams', None)
            if streams is None:
                # Raw playback and third-party connection shims predate the durable
                # stream registry, so preserve their historical reset behavior.
                self._reset()
            else:
                for stream in streams:
                    exchange_symbol, _, channel = stream.partition('@')
                    if channel.startswith('depth'):
                        exchange_symbol = exchange_symbol.upper()
                        normalized_symbol = self.exchange_symbol_mapping.get(exchange_symbol)
                        if normalized_symbol:
                            self._clear_symbol_state(normalized_symbol, exchange_symbol)
        if self.requires_authentication:
            create_task(self._refresh_token())
