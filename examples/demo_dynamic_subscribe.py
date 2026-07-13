'''
Manually validate runtime Binance Futures symbol subscriptions.

The feed starts with BTC and ETH, adds SOL and XRP after 30 seconds, removes
ETH after another 60 seconds, prints per-symbol counters, and exits.
'''
import asyncio
from collections import defaultdict

from cryptofeed import FeedHandler
from cryptofeed.defines import L2_BOOK, TRADES
from cryptofeed.exchanges import BinanceFutures


INITIAL_SYMBOLS = ['BTC-USDT-PERP', 'ETH-USDT-PERP']
ADDED_SYMBOLS = ['SOL-USDT-PERP', 'XRP-USDT-PERP']
REMOVED_SYMBOL = 'ETH-USDT-PERP'

counters = defaultdict(lambda: defaultdict(int))


async def book_callback(book, receipt_timestamp):
    counters[book.symbol]['l2_book'] += 1
    print(f'L2_BOOK {book}')


async def trade_callback(trade, receipt_timestamp):
    counters[trade.symbol]['trades'] += 1
    print(f'TRADE {trade}')


def print_summary(symbols):
    print('\nDynamic subscription summary')
    print(f'{"symbol":<20} {"l2_book":>12} {"trades":>12}')
    print(f'{"-" * 20} {"-" * 12} {"-" * 12}')
    for symbol in symbols:
        print(f'{symbol:<20} {counters[symbol]["l2_book"]:>12} {counters[symbol]["trades"]:>12}')


async def subscription_timeline(feed, loop):
    await asyncio.sleep(30)
    print(f'\nAdding symbols at runtime: {", ".join(ADDED_SYMBOLS)}')
    await feed.add_symbols(ADDED_SYMBOLS)

    await asyncio.sleep(60)
    print(f'\nRemoving symbol at runtime: {REMOVED_SYMBOL}')
    await feed.remove_symbols([REMOVED_SYMBOL])
    print_summary(INITIAL_SYMBOLS + ADDED_SYMBOLS)
    loop.stop()


def main():
    handler = FeedHandler()
    feed = BinanceFutures(
        symbols=INITIAL_SYMBOLS,
        channels=[L2_BOOK, TRADES],
        callbacks={L2_BOOK: book_callback, TRADES: trade_callback},
    )
    handler.add_feed(feed)

    # py3.11+/uvloop: get_event_loop() no longer creates a loop implicitly.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(subscription_timeline(feed, loop))
    handler.run(start_loop=True)


if __name__ == '__main__':
    main()
