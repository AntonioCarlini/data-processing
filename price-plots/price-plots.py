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
#   python price-plots.py BTC ETH 180 # plots BTC against ETH over the last 180 days


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
#
# Any error from CoinGecko (i.e. anything other than an HTTP 200 response) causes an immediate exit.
def build_symbols_to_coingecko_name_map():
    sym_to_cg_name = {}
    url = "https://api.coingecko.com/api/v3/coins/list"
    r = requests.get(url)
    if r.status_code != 200:
        print("CoinGecko returned an error:", r)
        exit(1)
    else:
        identifiers = r.json()
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

    parser.add_argument('first_crypto', nargs='?', default="BTC")
    parser.add_argument('second_crypto', nargs='?', default="FLOW")
    parser.add_argument('days', type=int, nargs='?', default=90)
    parser.add_argument('-d', '--debug', action='store_true')
    args = parser.parse_args()

    # Build map that allows conversion from symbol name (e.g. "BTC") to the coingecko name (in this case, "bitcoin")
    sym_to_name = build_symbols_to_coingecko_name_map()

    # Construct the string that determines how much data to gather e.g. if 100 days is specified on the CLI, build the string "100days"
    time_span = '{}days'.format(args.days)

    # Dump debug info if requested
    if args.debug:
        print("coin #1:    ", args.first_crypto)
        print("coin #1:    ", args.second_crypto)
        print("days:       ", args.days)
        print("cg name #1: ", sym_to_name[args.first_crypto.lower()])
        print("cg name #2: ", sym_to_name[args.second_crypto.lower()])
        print("time_span:  ", time_span)

    # Convert CLI coin symbols (converted to lowercase) to CoinGecko names.
    coin_1 = sym_to_name[args.first_crypto.lower()]
    coin_2 = sym_to_name[args.second_crypto.lower()]

    # Initialize CoinGecko API client
    coinGecko = pycoingecko.CoinGeckoAPI()

    # Get historical price data for the two coins
    coin_1_data = coinGecko.get_coin_market_chart_by_id(coin_1, 'usd', time_span)
    coin_2_data = coinGecko.get_coin_market_chart_by_id(coin_2, 'usd', time_span)

    # Extract the dates and prices from the data
    coin_1_dates = [data[0] for data in coin_1_data['prices']]
    coin_2_dates = [data[0] for data in coin_2_data['prices']]

    if args.debug:
        print("api return:    ", coin_1_data)
        print()
        print()
        print("api return prices: ", coin_1_data['prices'])
        print()
        print()
        print("coin_1_dates:  ", coin_1_dates)

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

    if args.debug:
        print("coin_1_dates (date/time):  ")
        for date in coin_1_dates:
            print(date)
            print()
            print()
            print("coin_1_prices:  ", coin_1_prices)
            print()
            print()
            print("API keys: ", list(coin_1_data))

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
    plt.plot(coin_2_dates[-args.days:], coin_1_to_coin_2_ratio[-args.days:])
    plt.xlabel('Date')
    plt.ylabel('Price (USD)')
    plt.title('Historical {sym_1}/{sym_2}'.format(sym_1=args.first_crypto, sym_2=args.second_crypto))

    plt.show()

    # Wait for a keypress before removing the figures
    input()

# Invoke the main function
main()
