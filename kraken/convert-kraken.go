package main

// Placeholder for kraken processor, based on crypto.com

// "deposit"
// fiat (ZGBP or ZEUR) with no txid is a fiat deposit. checked but not converted
// 	crypto: indicates a TRANSFER-IN if there is a refid and txid (ignore if n txid, error if no refid)

// "spend"
// Must be either ZGBP or EUR.HOLD
// Find matching "receive" with same refid, which should be for crypo.
// these two together build a BUY transaction

// "receive"
// for crypto, usually matched with a "spend"?

// "withdrawl"
// so far must be crypto
// may be matched with a "transfer", in which case this is a staking event

// "staking"

// This program takes a CSV from crypto.com and adjusts it into a format suitable for my records.
// Where applicable it rearranges each row into a format that matches my exchange-independent records.
// Each row entry that is expected to have a meaningful value is checked and errors are flagged on the command line.
// Some rows do not produce any output but as much checking as possible is performed anyway to try to avoid silent corruption.
//
// Output is grouped by cryptocurrency and presented in ascending date order within each group.

// Usage:
// The program takes two parameters: the input transactions (in CSV format) and a file into which to write the resulting converted transactions (in CSV format).
//
// One way to run it would be:
//    go run convert-cdc.go crypto_dot_com.csv standard_transactions.csv

// Notes:
// Timestamps are in UTC. These are converted to UK local time. (Currently no conversion is necessary but it will be necessary starting in March 2022).

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
type ledger struct {
	row     int
	txid    string
	refid   string
	time    string
	format  string
	subtype string
	aclass  string
	asset   string
	amount  string
	fee     string
	balance string
}

// Open the input file, convert it to the output format and write it out in CSV format
func main() {

	flag.Parse()

	inputs := flag.Args()
	if len(inputs) != 2 {
		log.Fatalf("Exactly 2 arguments required but %d supplied\n", len(inputs))
	}

	transactionsFilename := flag.Arg(0)
	outputFile := flag.Arg(1)

	transactions := readTransactions(transactionsFilename)

	convertedTransactions := convertTransactions(transactions)

	writeConvertedTransactions(outputFile, convertedTransactions)
}

func readTransactions(name string) [][]string {
	f, err := os.Open(name)
	if err != nil {
		log.Fatalf("Cannot open '%s': %s\n", name, err.Error())
	}
	defer f.Close()

	r := csv.NewReader(f)

	transactions, err := r.ReadAll()
	if err != nil {
		log.Fatalln("Cannot read CSV data:", err.Error())
	}

	return transactions
}

// Works through every line of the input transactions file and converts each to the expected format or discards it.
// Every line of the input file is parsed even though not all of them produce a corresponding line in the output file.
// At the end each cryptocurrency's data is gathered together in forward time order (crypto.com lists transactions in reverse time order).
func convertTransactions(transactions [][]string) [][]string {
	// The first element must match this exactly otherwise the format may have changed:
	expectedFirstRow := []string{"txid", "refid", "time", "type", "subtype", "aclass", "asset", "amount", "fee", "balance"}

	firstRow := transactions[0]

	if !testSlicesEqual(firstRow, expectedFirstRow) {
		log.Fatalf("First row fails to match")
	}

	output := make(map[string][][]string, 0)
	pendingSpends := make(map[string]ledger)
	pendingWithdrawals := make(map[string]ledger)
	pendingStakingDeposits := make(map[string]ledger)

	for i, row := range transactions[1:] {
		csvRowIndex := i + 2
		entry := ledger{csvRowIndex, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]}

		ukTime := convertKrakenToUKTime(entry.time)
		rowValuesAcceptable := areRowValuesAcceptable(entry)

		if entry.format == "deposit" {
			// If fiat currency, then it can be ignored (only "ZGBP" or "ZEUR" or "EUR.HOLD" will be seen here).
			// If the currency ends in ".S" then this is the staking version of the currency, so save it to later match against a "transfer".
			// Otherwise this is a TRANSFER-IN of that currency.
			valid := rowValuesAcceptable
			if entry.asset == "ZGBP" || entry.asset == "ZEUR" || entry.asset == "EUR.HOLD" {
				// This is a fiat currency deposit and so does not need to be processed further
			} else if strings.HasSuffix(entry.asset, ".S") {
				// This is a deposit of a staked currency (e.g. FLOW.S) which should later be matched by a "transfer"
				if prev, found := pendingStakingDeposits[entry.refid]; found {
					fmt.Printf("Saw deposit of staked currency with repeated refid: %s (previous in row %d)\n", entry.refid, prev.row)
				}
				pendingStakingDeposits[entry.refid] = entry
			} else {
				if valid {
					data := []string{"", "Kraken", entry.time, ukTime, entry.amount, "", "", "", "", "", "", "", "", "TRANSFER-IN"}
					output[entry.asset] = append(output[entry.asset], data)
				} else {
					data := []string{"**BAD DATA", "Kraken", entry.time, ukTime, entry.amount, "", "", "", "", "", "", "", "", "TRANSFER-IN **BAD DATA"}
					output[entry.asset] = append(output[entry.asset], data)
				}
			}
		} else if entry.format == "spend" {
			// Simply preserve this in the spending map
			// The only reasonable check is to make sure that the reference number is not already in the map
			// TODO check txid not blank and format is valid
			// TODO check subtype is blank
			// TODO check that balance is not blank
			if prev, found := pendingSpends[entry.refid]; found {
				fmt.Printf("Saw spend with repeated refid: %s (previous in row %d)\n", entry.refid, prev.row)
			}
			pendingSpends[entry.refid] = entry
		} else if entry.format == "receive" {
			// Find the corresponding "spend" and use it to fill in the "BUY"
			// Note that the actual spend is the amount plus the fee!
			// Complain if the reference number is not already in the map
			// TODO check txid not blank and format is valid
			// TODO check subtype is blank
			// TODO check that balance is not blank
			// TODO handle a non-GBP spend
			valid := true
			spend, found := pendingSpends[entry.refid]
			if !found {
				fmt.Printf("Saw receive with no matching spend refid: %s (previous in row %d)\n", spend.refid, spend.row)
				valid = false
			}
			totalSpend := calculateSpendAsString(spend)
			if valid {
				data := []string{"", "Kraken", entry.time, ukTime, entry.amount, "", "", "", totalSpend, "", "", "", "", "BUY"}
				output[entry.asset] = append(output[entry.asset], data)
			} else {
				data := []string{"**BAD DATA**", "Kraken", entry.time, ukTime, entry.amount, "", "", "", totalSpend, "", "", "", "", "BUY **BAD DATA**"}
				output[entry.asset] = append(output[entry.asset], data)
			}
			delete(pendingSpends, entry.refid)
		} else if entry.format == "withdrawal" {
			// TODO Comes in two types:
			// TODO (a) first has no txid, second has txid; asset, amount and fee must match; use time from second
			// TODO (b) first has no txid, matches a transfer (spottostaking), asset must match and must not be a staked asset (no trailing .S)
			// TODO So if there is no txid, simply add it to the pending list.
			// If there is a txid, there must be a pending withdrawal with a matching refid.
			if entry.txid == "" {
				pendingWithdrawals[entry.refid] = entry
			} else {
				valid := true
				var withdrawal ledger
				withdrawal, valid = pendingWithdrawals[entry.refid]
				if (entry.amount != withdrawal.amount) || (entry.fee != withdrawal.fee) || (entry.asset != withdrawal.asset) {
					valid = false
					fmt.Printf("withdrawl on row %d does not properly match withdrawal on row %d\n", entry.row, withdrawal.row)
				}
				if valid {
					data := []string{"", "Kraken", entry.time, ukTime, entry.amount, "", "", "", "", "", "", "", "", "TRANSFER-OUT"}
					output[entry.asset] = append(output[entry.asset], data)
				} else {
					data := []string{"**BAD DATA**", "Kraken", entry.time, ukTime, entry.amount, "", "", "", "", "", "", "", "", "STAKING **BAD DATA**"}
					output[entry.asset] = append(output[entry.asset], data)
				}
				delete(pendingWithdrawals, entry.refid)
			}
		} else if entry.format == "transfer" {
			// "transfer" is used to move a cryptocurrency into a staking pool, so it never produces any output
			// TODO subtype must be either "spottostaking" or "stakingfromspot"
			// TOOD subtype "spottostaking" must be matched with a pending withdrawal
			// TODO subtype "stakingfromspot" must be matched with a pending deposit
			// TODO txid must not be blank
			// TODO balance must not be blank
			// TODO: may be matched with a previous "withdrawal", in which case it represents an initial move into staking
			if entry.subtype == "spottostaking" {
				// This entry (and the matching "withdrawal") represent a move of a cryptoasset to the staking pool
				// No output row will be written. The matching "withdrawal" must be found, checked and removed from the pending withdrawals.
				valid := true
				var withdrawal ledger
				withdrawal, valid = pendingWithdrawals[entry.refid]
				if !valid {
					fmt.Printf("transfer on row %d has no matching withdrawal\n", entry.row)
				} else if (entry.amount != withdrawal.amount) || (entry.fee != withdrawal.fee) || (entry.asset != withdrawal.asset) {
					fmt.Printf("transfer on row %d does not properly match withdrawal on row %d\n", entry.row, withdrawal.row)
				}
				delete(pendingWithdrawals, entry.refid)
			} else if entry.subtype == "stakingfromspot" {
				// TODO make sure there is a pending staking deposit for this
				if _, found := pendingStakingDeposits[entry.refid]; !found {
					// TODO
					fmt.Printf("transfer stakingfromspot with no matching deposit on row %d\n", entry.row)
				} else {
					// TODO this is matched so it is OK
					delete(pendingStakingDeposits, entry.refid)
				}
			} else {
				fmt.Printf("Invalid subtype for transfer on row %d\n", entry.row)
			}
		} else if entry.format == "staking" {
			// TODO tidy up but otherwise all is complete
			valid := rowValuesAcceptable
			stakedCurrency := strings.TrimSuffix(entry.asset, ".S")
			if stakedCurrency == entry.asset {
				valid = false
				fmt.Printf("row %d, staking asset does not have .S suffix: %s\n [%s]\n", csvRowIndex, entry.asset, row)
			}
			// Look for a pending deposit that matches the currency and the amount and has a blank txid.
			// If such an entry is found, remove it from the pending deposits
			foundDeposit := false
			for k, v := range pendingStakingDeposits {
				if v.asset == entry.asset && v.amount == entry.amount && v.txid == "" {
					delete(pendingStakingDeposits, k)
					foundDeposit = true
					break
				}
			}
			if !foundDeposit {
				fmt.Printf("Failed to find corresponding deposit for staking on row %d\n", entry.row)
			}
			if valid {
				data := []string{"", "Kraken", entry.time, ukTime, entry.balance, "", "", "", "", "", "", "", "", "STAKING"}
				output[stakedCurrency] = append(output[stakedCurrency], data)
			} else {
				data := []string{"**BAD DATA**", "Kraken", entry.time, ukTime, entry.balance, "", "", "", "", "", "", "", "", "STAKING **BAD DATA**"}
				output[stakedCurrency] = append(output[stakedCurrency], data)
			}
		} else {
			fmt.Printf("UNRECOGNISED <%s>\n", entry.format)
			// entry := []string{"***UNRECOGNISED***", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "***INVALID***"}
			// output[currency] = append(output[currency], entry)
		}
	}

	// Warn if there any unmatched spends
	for _, v := range pendingSpends {
		fmt.Printf("Error: Unmatched \"spend\": row: %d entry=%v\n", v.row, v)
	}

	// Warn if there any unmatched withdrawals
	for _, v := range pendingWithdrawals {
		fmt.Printf("Error: Unmatched \"withdrawal\": row: %d entry=%v\n", v.row, v)
	}

	// Warn if there any unmatched deposits into staking
	for _, v := range pendingStakingDeposits {
		fmt.Printf("Error: Unmatched \"deposit\" (staking): row: %d entry=%v\n", v.row, v)
	}

	// Find all the currencies in the map
	// For some reason BTC is recorded as XXBT, ETH as XETH and DOGE as XXDG, so allow for this
	currencyTranslation := map[string]string{"XXBT": "BTC", "XXDG": "DOGE", "XETH": "ETH"}
	currencies := make([]string, 0)
	for k := range output {
		if replacement, found := currencyTranslation[k]; found {
			k = replacement
		}
		currencies = append(currencies, k)
	}
	sort.Strings(currencies)

	// Loop through currencies and produce an output that contains all the data categorised by currency
	// If the currency is not found that's because here we're looping through the corrected versions of the currencies (i.e. BTC and not XXBT),
	// so in that case find the original currency the hard way by doing a reverse lookup in the currencyTranslation map
	finalOutput := make([][]string, 0)
	for _, c := range currencies {
		data, found := output[c]
		if !found {
			for originalCurrency, translatedCurrency := range currencyTranslation {
				if translatedCurrency == c {
					data, found = output[originalCurrency]
					if !found {
						fmt.Printf("Failed to find data for translated currency %s (originally %s)\n", c, originalCurrency)
					}
					break
				}
			}
		}
		// Append the data and a prefix/postfix to the overall output
		finalOutput = append(finalOutput, []string{"", ""})
		finalOutput = append(finalOutput, []string{"", ""})
		finalOutput = append(finalOutput, []string{c, "Data for a fixed currency"})
		for _, v := range data {
			finalOutput = append(finalOutput, v)
		}
		finalOutput = append(finalOutput, []string{"", ""})
	}

	return finalOutput
}

// Writes out the accumulated data to the output file in CSV format
func writeConvertedTransactions(filename string, data [][]string) {

	f, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Cannot open '%s': %s\n", filename, err.Error())
	}

	defer func() {
		e := f.Close()
		if e != nil {
			log.Fatalf("Cannot close '%s': %s\n", filename, e.Error())
		}
	}()

	w := csv.NewWriter(f)
	err = w.WriteAll(data)
}

// Checks that two slices are identical.
// Checks that:
//  * the number of elements is identical
//  * the corresponding elements match exactly
func testSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		fmt.Printf("slice diff len: len-a %d len-b: %d\n", len(a), len(b))
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			fmt.Printf("slice mismatch at %d: <%s> vs <%s>\n", i, a[i], b[i])
			return false
		}
	}
	return true
}

// Converts from Kraken Exchange time to UK local time.
// As the Kracken exchange time in the ledger is unknown, currently nothing is done.

// During these dates (from https://www.gov.uk/when-do-the-clocks-change) the UK runs on GMT+1:
//
// 2020 	29 March 	25 October
// 2021 	28 March 	31 October
// 2022 	27 March 	30 October
// 2023 	26 March 	29 October
//
// In practice the next BST date is in NOV-2021 so at least until 27-MAR-2022 no conversion needs to happen until then.
func convertKrakenToUKTime(utcTime string) string {
	layout := "2006-01-02 15:04:05"
	t, err := time.Parse(layout, utcTime)
	if err != nil {
		fmt.Println(err)
	}
	nextBST := time.Date(2022, 3, 27, 1, 0, 0, 0, time.UTC)
	if t.After(nextBST) {
		t = t.Add(time.Hour * 1)
		log.Fatalf("Adjust code to handle incursion into 2022 BST")
	}
	result := t.Format(layout)
	if err != nil {
		fmt.Println(err)
	}
	return result
}

// Check the supplied row values match expected values.
// In the event of a problem, write to stdout and return false (i.e. not OK)
// Must be present: "refid", "time", "type", "aclass", "asset", "amount", "fee"

func areRowValuesAcceptable(entry ledger) bool {
	valid := true
	if entry.refid == "" || entry.time == "" || entry.format == "" || entry.asset == "" || entry.amount == "" || entry.fee == "" {
		fmt.Printf("ledger entry row %d has invalid empty entry\n", entry.row)
		valid = false
	}
	if entry.aclass != "currency" {
		fmt.Printf("ledger entry row %d has invalid 'aclass'\n", entry.row)
		valid = false
	}
	return valid
}

// Accepts a string representing a currency with two decimal places (e.g. GBP, EUR or USD) and returns the integer value in pennies.
// Allows for the cases where only one penny digit or no penny digits or no decimal point are present.
// The decimal comma notation is not supported (as it is not needed).
// The number of pennies digits can exceed two, so
// So:
//   "123.75" produces 12375
//   "123.7"  produces 12370
//   "123."   produces 12300
//   "123"    produces 12300
//   ".1"     produces    10

func makePenniesFromGBP(currency string) int {
	result := strings.Split(currency, ".")
	poundsString := result[0]
	if poundsString == "" {
		poundsString = "0"
	}
	penniesString := "00"
	if len(result) == 2 {
		penniesString = result[1]
	} else if len(result) > 2 {
		fmt.Printf("number of decimal separators exceeds 1 in %s\n", currency)
	}

	pounds, err := strconv.Atoi(poundsString)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	if len(penniesString) == 0 {
		penniesString = "00"
	} else if len(penniesString) == 1 {
		penniesString += "0"
	} else if len(penniesString) > 2 {
		penniesString = penniesString[0:2]
	}
	pennies, err := strconv.Atoi(penniesString)
	if err != nil {
		fmt.Println(err)
		// os.Exit(3)
	}
	return (pounds * 100) + pennies
}

// Helper function that calculates the total spend represented by a "spend" ledger entry
// Note that the spend.amount will usually be negative and the spend.fee will be positive.
// The result should be the addition of the absolute values, returned as a string.
// The entries often contain pennies values to more than two digits. These are simply truncated,
// although some care is taken to avoid floating point rounding errors.
func calculateSpendAsString(spend ledger) string {
	spendAmount := strings.TrimLeft(spend.amount, "-")
	spendFee := strings.TrimLeft(spend.fee, "-")
	amountPennies := makePenniesFromGBP(spendAmount)
	feePennies := makePenniesFromGBP(spendFee)
	totalPennies := amountPennies + feePennies
	finalPounds := totalPennies / 100
	finalPennies := totalPennies - (finalPounds * 100)
	return fmt.Sprintf("%s.%02.02s", strconv.Itoa(finalPounds), strconv.Itoa(finalPennies))
}
