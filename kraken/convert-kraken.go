package main

// Kraken exchange ledger CSV processor, based on crypto.com

// This program takes a CSV from Kraken and adjusts it into a format suitable for my records.
// Where applicable it rearranges each row into a format that matches my exchange-independent records.
// Each row entry that is expected to have a meaningful value is checked and errors are flagged on the command line.
// Some rows do not produce any output but as much checking as possible is performed anyway to try to avoid silent corruption.
//
// Output is grouped by cryptocurrency and presented in ascending date order within each group.

// Usage:
// The program takes two parameters: the input transactions (in CSV format) and a file into which to write the resulting converted transactions (in CSV format).
//
// One way to run it would be:
//    go run convert-kraken.go kraken.csv standard_transactions.csv
//
// Some official information about the ledger format can be found here:
//   https://support.kraken.com/hc/en-us/articles/360001169383-How-to-interpret-Ledger-history-fields
// An explanation of why some ledger entries are duplicated can be found here:
//   https://support.kraken.com/hc/en-us/articles/360001169443-Why-there-are-duplicate-entries-for-deposits-withdrawals
//
// Notes regarding how transactions are recorded in the ledger.
// These are based partly on the documentation but mainly on observation of transactions in the ledger.
//
// Deposit of fiat currency into kraken
//    - "deposit" or ZGBP or ZEUR or EUR.HOLD with blank txid and refid REF-A
//    - "deposit" or ZGBP or ZEUR or EUR.HOLD with blank txid and refid REF-A
//       (in the case of ZEUR, the second deposit will be for EUR.HOLD)
//
// Deposit of a token into kraken
//    - "deposit" of token with refid REF-A and blank txid and blank balance
//    - "deposit" of token with refid REF-A and non-blank txid and non-blank balance
//
// Purchase of a token:
//    - "spend" of fiat with refid REF-A
//    - "receive" of token with refid REF-A
//
// Staking of a token
//    - "withdrawal" of token with blank txid and refid REF-A
//    - "deposit" of token.S with blank txid and refid REF-B
//    - "transfer" of token.S with refid REF-B and and subtype "stakingfromspot"
//    - "transfer" of token with refid REF-A and subtype "spottostaking"
//
// Staking reward:
//    - "deposit" of token.S with blank balance
//    - "staking" of token.S with non-blank balance
//
//  Withdrawal of fiat currency from kraken
//    TBD
//
//  This leads to the following observations about the format field:
//
//  "deposit":    seen when depositing fiat or tokens, staking and receving staking rewards.
//  "withdrawal": seen when staking a token
//  "transfer":   seen when staking a token
//  "spend":      seen when purchasing a token
//  "receive":    seen when purchasing a token
//  "staking":    seen when receiving a staking rewards
//

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
	pendingTokenDeposits := make(map[string]ledger)

	for i, row := range transactions[1:] {
		csvRowIndex := i + 2
		entry := ledger{csvRowIndex, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]}

		ukTime := convertKrakenTimeToUKTime(entry.time)
		rowValuesAcceptable := areRowValuesAcceptable(entry)

		switch entry.format {
		case "deposit":
			// TBD: ensure that this code checks everything that is documented
			// If fiat currency, then it can be ignored (only "ZGBP" or "ZEUR" or "EUR.HOLD" will be seen here).
			// If the currency ends in ".S" then this is the staking version of the currency, so save it to later match against a "transfer".
			// Otherwise this is a TRANSFER-IN of that currency.
			valid := rowValuesAcceptable
			if isFiatCurrency(entry.asset) {
				// This is a fiat currency deposit and so does not need to be processed further
				// TODO: check fiat deposits more thoroughly
			} else if strings.HasSuffix(entry.asset, ".S") {
				// This is either:
				//   a request to stake currency (e.g. FLOW.S) which should later be matched by a "transfer" with the same refid
				// or
				//   a staking reward which should later be matched by a "staking" with matching details
				if prev, found := pendingStakingDeposits[entry.refid]; found {
					fmt.Printf("Saw deposit of staked currency with repeated refid: %s (previous in row %d)\n", entry.refid, prev.row)
				}
				pendingStakingDeposits[entry.refid] = entry
			} else {
				// This is a deposit of a token into the Kraken wallet.
				// Kraken lists these twice: firstly with a blank txid and a blank balance, and a second time with identical details but non-blank txid and balance. The same refid is used.
				// Store the first entry in pendingTokenDeposist and check that it is there when the second entry is seen. Only second entry triggers an output.
				// If only one but not both of txid and balance is blank, this is an unexpected error.
				if (entry.txid == "" && entry.balance != "") || (entry.txid != "" && entry.balance == "") {
					valid = false
				}
				if valid {
					if entry.txid == "" && entry.balance == "" {
						// This is the first of two expected deposits relating to a token. Store it for later processing.
						pendingTokenDeposits[entry.refid] = entry
					} else {
						if prev, found := pendingTokenDeposits[entry.refid]; !found {
							fmt.Printf("Saw deposit of token in row %d with without preparatory deposit\n", entry.row)
						} else {
							if (entry.asset != prev.asset) || (entry.amount != prev.amount) || (entry.fee != prev.fee) {
								fmt.Printf("Saw matching deposit of token from row %d with values that do not match row %d)\n", prev.row, entry.row)
							} else {
								data := []string{"", "Kraken", entry.time, ukTime, entry.amount, "", "", "", "", "", "", "", "", "TRANSFER-IN"}
								output[entry.asset] = append(output[entry.asset], data)
							}

						}
					}
				} else {
					data := []string{"**BAD DATA", "Kraken", entry.time, ukTime, entry.amount, "", "", "", "", "", "", "", "", "TRANSFER-IN **BAD DATA"}
					output[entry.asset] = append(output[entry.asset], data)
				}
			}
		case "spend":
			// This entry only occurs as part of a token purchase.
			// This entry provides details of the fiat currency used to purchase the token and the amount plus fee charged.
			// The balance will be the amount of this fiat currency remaining after the transaction.
			// Preserve this in the spending map so the data contained can be used when the corresponding "receive" is seen.
			// One check is to make sure that the reference number is not already in the map
			if prev, found := pendingSpends[entry.refid]; found {
				fmt.Printf("Saw spend with repeated refid: %s (previous in row %d)\n", entry.refid, prev.row)
			}
			// Check txid not blank and format is valid
			// Check subtype is blank
			// Check that balance is not blank
			// This will be re-checked later but report it now in case no correspdonding receive is seen
			if entry.txid == "" || entry.subtype != "" || entry.balance == "" {
				fmt.Printf("Saw 'spend' with missing fields in row %d\n", entry.row)
			}
			// Save the entry in the pendingSpends map for later use by a "receive"
			pendingSpends[entry.refid] = entry
		case "receive":
			// Find the corresponding "spend" and use it to fill in the "BUY"
			// Note that the actual spend is the amount plus the fee!
			valid := true
			spend, found := pendingSpends[entry.refid]
			// Complain if the reference number is not already in the map
			if !found {
				fmt.Printf("Saw 'receive' in row %d with no matching spend)\n", entry.row)
				valid = false
			} else {
				totalSpend := calculateSpendAsString(spend)
				// Perform some checks for both the "receive" and the "spend" entries
				// Check txid not blank and format is valid
				// Check subtype is blank
				// Check that balance is not blank
				if entry.txid == "" || entry.subtype != "" || entry.balance == "" {
					fmt.Printf("Saw 'receive' with missing fields in row %d\n", entry.row)
					valid = false
				}
				if spend.txid == "" || spend.subtype != "" || spend.balance == "" {
					fmt.Printf("Saw 'spend' with missing fields in row %d\n", entry.row)
				}
				// TODO handle a non-GBP spend; for now just flag it
				if spend.asset != "ZGBP" {
					fmt.Printf("Saw non GBP (currency %s) 'spend' in row %d\n", spend.asset, spend.row)
				}
				if valid {
					data := []string{"", "Kraken", entry.time, ukTime, entry.amount, "", "", "", totalSpend, "", "", "", "", "BUY"}
					output[entry.asset] = append(output[entry.asset], data)
				} else {
					data := []string{"**BAD DATA**", "Kraken", entry.time, ukTime, entry.amount, "", "", "", totalSpend, "", "", "", "", "BUY **BAD DATA**"}
					output[entry.asset] = append(output[entry.asset], data)
				}
				// Remove the "spend" entry that has now been used
				delete(pendingSpends, entry.refid)
			}
		case "withdrawal":
			// TBD: ensure that this code checks everything that is documented
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
		case "transfer":
			// TBD: ensure that this code checks everything that is documented
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
				fmt.Printf("Invalid subtype (%s) for transfer on row %d\n", entry.subtype, entry.row)
			}
		case "staking":
			// TBD: ensure that this code checks everything that is documented
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
				data := []string{"", "Kraken", entry.time, ukTime, entry.amount, "", "", "", "", "", "", "", "", "STAKING"}
				output[stakedCurrency] = append(output[stakedCurrency], data)
			} else {
				data := []string{"**BAD DATA**", "Kraken", entry.time, ukTime, entry.amount, "", "", "", "", "", "", "", "", "STAKING **BAD DATA**"}
				output[stakedCurrency] = append(output[stakedCurrency], data)
			}
		case "trade":
			// TBD
			log.Fatalf("row %d: unhandled transaction type %s", entry.row, entry.format)
		case "margin trade":
			// TBD
			log.Fatalf("row %d: unhandled transaction type %s", entry.row, entry.format)
		case "rollover":
			// TBD
			log.Fatalf("row %d: unhandled transaction type %s", entry.row, entry.format)
		case "adjustment":
			// TBD
			log.Fatalf("row %d: unhandled transaction type %s", entry.row, entry.format)
		case "settled":
			// TBD
			log.Fatalf("row %d: unhandled transaction type %s", entry.row, entry.format)
		case "reward":
			// TBD
			// This is documented as:
			//    "reward" = credit of staking rewards
			// and an undocumented format of "staking" does appear, so this is probably
			// a documentation error and is intended to be "staking".
			log.Fatalf("row %d: unhandled transaction type %s", entry.row, entry.format)
		case "sale":
			// TBD
			log.Fatalf("row %d: unhandled transaction type %s", entry.row, entry.format)
		default:
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
// The Kraken ledger is documented to record the time in UTC.
func convertKrakenTimeToUKTime(utcTime string) string {
	layout := "2006-01-02 15:04:05"
	t, err := time.Parse(layout, utcTime)
	if err != nil {
		fmt.Printf("Error parsing time layout for time %s: %s\n", utcTime, err)
	}
	location, err := time.LoadLocation("Europe/London")
	if err != nil {
		fmt.Printf("Error loading time location %s\n", err)
	}
	return t.In(location).Format("2006-01-02 15:04:05")
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
		// "aclass" is documentd as: "Asset Class. Value is always "currency". Not a useful field."
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

// Helper function that indicates whether the entry currency is an expected fiat one.
// Note that the expected currencies are those that are known to have been used so
// if an entry unexpectedly appears for a fiat currency that has not been used before (e.g. JPY)
// it will be treated as a new token.
func isFiatCurrency(currency string) bool {
	acceptedFiatCurrencies := map[string]bool{
		"ZGBP":     true,
		"ZEUR":     true,
		"EUR.HOLD": true,
	}
	_, found := acceptedFiatCurrencies[currency]
	return found
}
