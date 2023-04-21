package main

// Functions in this source file are concerned with gathering historical price data from
// CoinGecko and managing that data.

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/superoo7/go-gecko/v3/types"
)

// How many days of historical data to request
var daysOfPriceHistory = 540

var perCoinHistoricalPrices map[string]map[string]float32

// This function is called to get a specific value
// Cache results per token, held in a map using the coin symbol ("BTC") not the CG name ("bitcoin")
// if the cache is empty then lookup using FetchCoinPrices
// if the cache is not empty but no value is found, return an error
// later on, make a single query to update the cache
//	// The free coingecko service has a rate limit on the API, so try to avoid hitting that
//	time.Sleep(8 * time.Second)
//	details, err := cg.CoinsIDHistory(lookupToken, cgDate, true)
//	if err != nil {
//		log.Fatal(err)
//	}
// optimisations:
// only sleep when required (i.e. allow N calls per minute)
//

// When a missing value is requested, if this variable is true, perform
// a further lookup on CoinGecko, otherwise simply average the preceding
// subsequent values.
var fillInWithLookups = false

func LookupHistoricalTokenValueInBulk(coinSymbol string, dateTime string, verbose bool) (float32, error) {
	var historicalData map[string]float32
	var ok bool
	historicalData, ok = perCoinHistoricalPrices[coinSymbol]
	if !ok {
		if verbose {
			fmt.Printf("coin %q has no historical data .. .fetching\n", coinSymbol)
		}
		cgCoinName, ok := ConvertCoinSymbolToCoingeckoCoinName(coinSymbol)
		if !ok {
			if verbose {
				fmt.Printf("BAD: %q => %q", coinSymbol, cgCoinName)
			}
			return -1.0, fmt.Errorf("Coin Value Lookup Error: Unrecognised symbol %q", coinSymbol)
		}
		prices, err := FetchCoinPrices(cgCoinName, daysOfPriceHistory, false)
		if err != nil {
			return -1.0, fmt.Errorf("Coin Value Retrieval Error: %q", err)
		}
		if perCoinHistoricalPrices == nil {
			perCoinHistoricalPrices = make(map[string]map[string]float32, 0)
		}
		perCoinHistoricalPrices[coinSymbol] = prices
		historicalData = prices
	}

	date := dateTime[0:10]
	price, ok := historicalData[date]
	if !ok {
		if fillInWithLookups {
			var err error
			price, err = LookupCoinValueAtGivenTime(coinSymbol, dateTime)
			if err != nil {
				return -1.0, fmt.Errorf("Coin Value Retrieval Error: %q", err)
			}
		} else {
			requestedDate, err := time.Parse("2006-01-02", dateTime[0:10])
			if err != nil {
				return -1.0, fmt.Errorf("Coin Value Retrieval Error: %q", err)
			}
			prevDay := requestedDate.AddDate(0, 0, -1)
			nextDay := requestedDate.AddDate(0, 0, 1)
			prevPrice, ok := historicalData[prevDay.Format("2006-01-02")]
			if !ok {
				return -1.0, fmt.Errorf("Coin Value Retrieval Error Prev Day: %q", err)
			}
			nextPrice, ok := historicalData[nextDay.Format("2006-01-02")]
			if !ok {
				return -1.0, fmt.Errorf("Coin Value Retrieval Error Next Day: %q", err)
			}
			price = (prevPrice + nextPrice) / 2.0
		}

		historicalData[date] = price
	}

	// Finally return the requested price
	return price, nil
}

// Given a date/time (in YYYY-MM-DD HH:MM:SS format) lookup a given coin's
// value at that moment in time (according to the CoinGeckoAPI)
func LookupCoinValueAtGivenTime(coinSymbol string, dateTime string) (float32, error) {
	fmt.Printf("Coin Value Lookup Error: no record for %q on %q\n", coinSymbol, dateTime)
	cgCoinName, ok := ConvertCoinSymbolToCoingeckoCoinName(coinSymbol)
	if !ok {
		fmt.Printf("BAD: %q => %q", coinSymbol, cgCoinName)
		return -1.0, fmt.Errorf("Coin Value Lookup Error: Unrecognised symbol %q", coinSymbol)
	}

	// Coingecko wants the date format in DD-MM-YYYY HH:MM:SS so convert to that
	parsedDate, err := time.Parse("2006-01-02 15:04:05", dateTime)
	if err != nil {
		return -1.0, fmt.Errorf("Coin Value Individual Lookup Error: unparseable date %q (%q) %q", dateTime, cgCoinName, err)
	}
	cgDate := parsedDate.Format("02-01-2006 15:04:05")
	RateLimitCoinGeckoApiCalls(false)
	details, err := cg.CoinsIDHistory(cgCoinName, cgDate, true)
	if err != nil {
		return -1.0, fmt.Errorf("Coin Value Individual Lookup Error: no CG response for %q (%q): %q", dateTime, cgCoinName, err)
	}
	return float32(details.MarketData.CurrentPrice["usd"]), nil
}

// Queries CoinGecko for a specific coin's price history over the number of days requested.
// The result is a map of "YYYY-MM-DD" => price.
// Because of the way CoinGecko returns data, there may be occasional gaps of one day in the data.
func FetchCoinPrices(coin string, days int, verbose bool) (map[string]float32, error) {
	RateLimitCoinGeckoApiCalls(false)
	data, err := cg.CoinsIDMarketChart(coin, "usd", strconv.Itoa(days))
	if err != nil {
		log.Fatal(err)
	}

	if verbose {
		fmt.Printf("Prices\n")
		for _, v := range *data.Prices {
			fmt.Printf("%s:    %.04f\n", time.Unix(int64(v[0])/1000, int64(v[0])%1000).UTC().Format("2006-01-02 15:04:05"), v[1])
			fmt.Printf("%T %T %T\n", v, v[0], v[1])
			break
		}
	}

	// The CoinGecko API returns: 'prices', 'market_caps' and 'total_volumes'
	// 'prices' is an array of [unix-timestamp, coin-price]
	//
	// The amount of data found for any given day depends on the number of days of data requested.
	// For any date with only one entry, use that entry.
	// For any date with more than one entry, average the entries and use the average value.
	return ConvertTimePriceHistoryToDailyPriceHistory(*data.Prices, verbose)
}

// Coingecko produces price history as an array of pairs values.
// The first value is a Unix timestamp.
// The second value is the price as a 32-bit floating point number.
//
// This function turns that array into a map of "YYYY-MM-DD" => price
// for every date on which a price is available.
//
// For those days where more than one price is available, the average is used.
//
// Dates are expected to be in ascending order with no gaps greater than two days.
func ConvertTimePriceHistoryToDailyPriceHistory(history []types.ChartItem, verbose bool) (map[string]float32, error) {
	dayOfYear := -1              // start with an impossible value
	var totalValue float32 = 0.0 // start with no daily total value
	pricesCounted := 0           // no prices counted towards the total
	thisDate := ""               // start with no recorded date
	results := make(map[string]float32, 0)

	for _, v := range history {
		dateTime := time.Unix(int64(v[0])/1000, int64(v[0])%1000)
		thisDOY := dateTime.YearDay()
		if dayOfYear == -1 {
			// This is the very first entry ever; set things up with a proper start
			dayOfYear = thisDOY
			pricesCounted = 1
			totalValue = v[1]
			thisDate = dateTime.Format("2006-01-02")
		} else if thisDOY == dayOfYear {
			// Same day as last time ... continue to accumulate values
			pricesCounted += 1
			totalValue += v[1]
		} else {
			// Save the calculated value
			if verbose {
				fmt.Printf("Here save the total value as %s => %.04f\n", thisDate, totalValue/float32(pricesCounted))
			}
			// Stop if a duplicate entry is about to be made
			if _, ok := results[thisDate]; ok {
				return make(map[string]float32, 0), fmt.Errorf("Duplicate value generated for %s, previous was %0.04f\n", thisDate, results[thisDate])
			}
			results[thisDate] = totalValue / float32(pricesCounted)

			// The day should always increase by one, allowing for a wraparound from DEC-31 to JAN-01.
			// However, CoinGecko's time intervals between samples when requesting longer time periods
			// turn out to be just over 24 hours (and not entirely consistent), so if one price is 23:59
			// on day N, the next price may be 00:02 on day N+2 and no price is provided for day N+1.
			// For the moment, just leave these gaps in the record and the lookup code will fill these
			// gaps in on demand if they turn out to be needed when processing the data.
			lastDate := thisDate[5:] // "thisDate" is the day that has just been processed; lop off the year "YYYY-"
			dayTransitionOK := false
			if (thisDOY == dayOfYear+1) || (thisDOY == dayOfYear+2) {
				dayTransitionOK = true
			} else if (thisDOY == 1) && ((lastDate == "12-30") || (lastDate == "12-31")) {
				dayTransitionOK = true
			} else if (thisDOY == 2) && (lastDate == "12-31") {
				dayTransitionOK = true
			}
			if !dayTransitionOK {
				return make(map[string]float32, 0), fmt.Errorf("Unexpected date/time: %s  (in full %s)  this-day: %d  prev-day: %d\n", thisDate, dateTime.Format("2006-01-02 15:04:05"), thisDOY, dayOfYear)
			}
			// Start processing the new day's prices
			dayOfYear = thisDOY
			pricesCounted = 1
			totalValue = v[1]
			thisDate = dateTime.Format("2006-01-02")
		}
		if verbose {
			fmt.Printf("date/time: %s  day-of-year: %3.3d  this value: %.04f prices: %4.4d  total: %.04f\n", dateTime.Format("2006-01-02 15:04:05"), thisDOY, v[1], pricesCounted, totalValue)
		}
	}

	// Remember to write out the currently in-progress calcultation
	if _, ok := results[thisDate]; ok {
		return make(map[string]float32, 0), fmt.Errorf("Duplicate value generated for %s, previous was %0.04f\n", thisDate, results[thisDate])
	}
	if verbose {
		fmt.Printf("Here save the total value as %s => %.04f\n", thisDate, totalValue/float32(pricesCounted))
	}
	results[thisDate] = totalValue / float32(pricesCounted)

	return results, nil
}

// Converts from a coin (ticker) symbol (such as BTC) to the name that Coingecko uses
// for that coin (such as BTC). Currently this is a simple map lookup. However
// CoinGecko does provide a list of pairs for conversion purposes so by isolating
// this functionality here a future upgrade should be less painful.
var token2cgToken = map[string]string{
	"ADA":   "cardano",
	"AVAX":  "avalanche-2",
	"AXS":   "axie-infinity",
	"BNB":   "bnb",
	"BSGG":  "betswap-gg",
	"BTC":   "bitcoin",
	"CRO":   "crypto-com-chain",
	"DOGE":  "dogecoin",
	"DOT":   "polkadot",
	"ENJ":   "enjincoin",
	"ETH":   "ethereum",
	"FLOW":  "flow",
	"FWT":   "freeway",
	"GOHM":  "governance-ohm",
	"MANA":  "decentraland",
	"MATIC": "matic-network",
	"NEXO":  "nexo",
	"SAND":  "the-sandbox",
	"SOL":   "solana",
	"TIME":  "wonderland",
	"WMEMO": "wrapped-memory",
}

func ConvertCoinSymbolToCoingeckoCoinName(symbol string) (string, bool) {
	lookupToken, found := token2cgToken[symbol]
	return lookupToken, found
}

// Implement a delay to avoid overloading the CoinGecko API.
// The limit is documented at: https://apiguide.coingecko.com/getting-started/error-and-rate-limit.
// It may be as low as 10 requests per minute.
//
// For now, no delays are imposed.
// Track the time (in sec from unix time) of each request in order in an array.
// Eliminate any leading requests that are older than 60s.
// If the resulting array is 8 or longer, delay by 8s.
var requestTimes []int64

// Set how far back in time (measured in days) to request
// coin price data from CoinGecko.
func SetDaysOfPriceHistoryToRequest(daysOfHistory int) {
	daysOfPriceHistory = daysOfHistory
}

var cgApiCallTimes []int64

func RateLimitCoinGeckoApiCalls(verbose bool) {
	if cgApiCallTimes == nil {
		cgApiCallTimes = make([]int64, 0)
	}

	secondsNow := time.Now().Unix()
	cgApiCallTimes = append(cgApiCallTimes, secondsNow)
	if verbose {
		fmt.Printf("Rate Limit Start:   %d calls ...\n", len(cgApiCallTimes))
	}
	// Count the number of API calls that are NOT within 60 seconds
	count := 0
	for _, v := range cgApiCallTimes {
		if verbose {
			fmt.Printf("Rate Limit Check: %d (+60=%d)%d ...\n", v, v+60, secondsNow)
		}

		if v+60 < secondsNow {
			count += 1 // Count an entry that is older than 60s
		} else {
			break // No need to count further once an entry is seen that is within one minute
		}
	}
	if count > 0 {
		cgApiCallTimes = cgApiCallTimes[count:]
	}
	if verbose {
		fmt.Printf("Rate Limit Trimmed: %d calls ...\n", len(cgApiCallTimes))
	}

	if len(cgApiCallTimes) > 6 {
		if verbose {
			fmt.Printf("%d calls within 1 min; pausing ...\n", len(cgApiCallTimes))
		}
		time.Sleep(8 * time.Second)
	}
}
