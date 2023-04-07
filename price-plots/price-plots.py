import argparse
import pycoingecko
import matplotlib.pyplot as plt
import datetime
import requests

#
# Given two cryptocurrencies and a number of days, this program uses the CoinGecko API to find the daily prices for
# each of the cryptocurrencies over the required number of days and produces separate plots of each crypto against
# time and also the ratio of the first crypto against the second over the same time period.
#
#  Usage:
#   python price-plots.py BTC ETH 90 # plots BTC against ETH over the last 90 days


# CoinGecko doesn't use the normal symbols (e.g. BTC) to identify a coin.
# Instead it uses its own name (e.g. bitcoin)
# By reading CoinGecko's coins/list URL we can get a JSON-formatted list of entries
# of the form:
#  {{'id': 'bitcoin', 'symbol': 'btc', 'name': 'Bitcoin'},{'id': 'zimbocash', 'symbol': 'zash', 'name': 'ZIMBOCASH'},...}
# From that we can build a map of symbol->name
#
# There is a slight problem in that the IDs are not unique (e.g. ETH is 'Ethereum' and 'Ethereum (Wormhole)' and perhaps a few more).
# Combat this by only replacing an existing entry if the new ID is shorter.
#
# Also enure that symbols are always entirely in lowercase so that matching against the symbol provided on the command line
# can be simplified.
def build_symbols_to_coingecko_name_map():
    url = "https://api.coingecko.com/api/v3/coins/list"
    r = requests.get(url)
    identifiers = r.json()
    sym_to_cg_name = {}
    for id in identifiers:
        symbol = id["symbol"].lower() 
        if not (symbol in sym_to_cg_name) or (len(id["id"]) < len(sym_to_cg_name[symbol])):
            sym_to_cg_name[symbol] = id["id"]
    return sym_to_cg_name

#
# This is the meat of the program.
# It translates the CLI coin symbols into the names used for those coins by CoinGecko and then queries CG
# for the prices of those coins over the last 365 days (which is something the API conveniently provides).
# The ratio of coin1/coin2 prices is generated over that same period of time.
# The matplotlib library is used to generate the three plots of data over the specified period.
def main():
    parser = argparse.ArgumentParser(
                    prog='plot-prices',
                    description='Plots graphs of two cryptocurrencies and their ratio')

    parser.add_argument('first_crypto', default="BTC")
    parser.add_argument('second_crypto', default="FLOW")
    parser.add_argument('days', type=int, default=90)
    args = parser.parse_args()

    sym_to_name = build_symbols_to_coingecko_name_map()

    print("coin #1: ", args.first_crypto)
    print("coin #1: ", args.second_crypto)
    print("days:    ", args.days)
    print("name #1: ", sym_to_name[args.first_crypto.lower()])
    print("name #2: ", sym_to_name[args.second_crypto.lower()])
    coin_1 = sym_to_name[args.first_crypto.lower()]
    coin_2 = sym_to_name[args.second_crypto.lower()]

    # Initialize CoinGecko API client
    coinGecko = pycoingecko.CoinGeckoAPI()

    # Get historical price data for the two coins
    coin_1_data = coinGecko.get_coin_market_chart_by_id(coin_1, 'usd', '365days')
    coin_2_data = coinGecko.get_coin_market_chart_by_id(coin_2, 'usd', '365days')

    # Extract the dates and prices from the data
    coin_1_dates = [data[0] for data in coin_1_data['prices']]
    coin_2_dates = [data[0] for data in coin_2_data['prices']]

    # convert unix timestamp to datetime
    coin_1_dates = [
        datetime.datetime.fromtimestamp(date/1000)
        for date in coin_1_dates
    ]
    coin_2_dates = [
        datetime.datetime.fromtimestamp(date/1000)
        for date in coin_2_dates
    ]

    coin_1_prices = [data[1] for data in coin_1_data['prices']]
    coin_2_prices = [data[1] for data in coin_2_data['prices']]


    # Plot the data
    plt.figure(figsize=(20,10))

    plt.subplot(2,2,1)
    plt.plot(coin_1_dates, coin_1_prices)
    plt.xlabel('Date')
    plt.ylabel('Price (USD)')
    plt.title('Historical {sym} ({name}) (USD)'.format(sym=args.first_crypto, name=coin_1))

    plt.subplot(2, 2, 2)
    plt.plot(coin_2_dates, coin_2_prices)
    plt.xlabel('Date')
    plt.ylabel('Price (USD)')
    plt.title('Historical {sym} ({name}) (USD)'.format(sym=args.second_crypto, name=coin_2))

    # generate coin_1/coin_2 ratio
    shorter_list_len = len(coin_2_prices)
    if len(coin_1_prices) < len(coin_2_prices):
        shorter_list_len = len(coin_1_prices)
        
    coin_1_to_coin_2_ratio = [coin_1_prices[i]/coin_2_prices[i] for i in range(shorter_list_len)]  

    plt.subplot(2, 2, 3)
    plt.plot(coin_2_dates[-90:], coin_1_to_coin_2_ratio[-90:])
    plt.xlabel('Date')
    plt.ylabel('Price (USD)')
    plt.title('Historical {sym_1}/{sym_2}'.format(sym_1=args.first_crypto, sym_2=args.second_crypto))

    plt.show()

    # Wait for a keypress before removing the figures
    input()

# Invoke the main function
main()
