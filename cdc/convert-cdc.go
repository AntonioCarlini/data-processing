package main

// This program takes a CSV from crypto.com and adjusts it into a format suitable for my records.
// Where applicable it rearranges each row into a format that matches my exchange-independent records.
// Each row entry that is expected to have a meaningful value is checked and errors are flagged on the command line.
// Some rows do not produce any output but as much checking as possible is performed anyway to try to avoid silent corruption.

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
	"strings"
	"time"
)

//
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
// At the end the order of the transactions is reversed (as crypto.com list them in reverse time order).
func convertTransactions(transactions [][]string) [][]string {
	// The first element must match this exactly otherwise the format may have changed:
	// expectedFirstRow := []string{"Transaction", "Type", "Currency", "Amount", "USD Equivalent", "Details", "Outstanding Loan", "Date / Time"}
	expectedFirstRow := []string{"Timestamp (UTC)", "Transaction Description", "Currency", "Amount", "To Currency", "To Amount", "Native Currency", "Native Amount", "Native Amount (in USD)", "Transaction Kind"}

	firstRow := transactions[0]

	if !testSlicesEqual(firstRow, expectedFirstRow) {
		log.Fatalf("First row fails to match")
	}

	output := make([][]string, 0)

	for i, row := range transactions[1:] {
		csvRowIndex := i + 2
		exchangeTime := row[0]
		description := row[1]
		currency := row[2]
		amount := row[3]
		// toCurrency := row[4]
		// toAmount := row[5]
		nativeCurrency := row[6]
		nativeAmount := row[7]
		//nativeAmountUSD := row[8]
		kind := row[9]

		ukTime := convertUtcToUKTime(exchangeTime)

		if description == "Sign-up Bonus Unlocked" {
			// Check the required values are as expected
			if !areRowValuesAcceptable(csvRowIndex, row, currency, "CRO", nativeCurrency, "USD", kind, "referral_gift", "", "") {
				fmt.Println("Bad value seen")
				entry := []string{"**BAD DATA**", "crypto.com App", exchangeTime, ukTime, amount, "", nativeAmount, "", "", "", "", "", "", "REWARD **BAD DATA**"}
				output = append(output, entry)
			} else {
				entry := []string{"", "crypto.com App", exchangeTime, ukTime, amount, "", nativeAmount, "", "", "", "", "", "", "REWARD"}
				output = append(output, entry)
			}
		} else if strings.HasSuffix(description, " Deposit") {
			// TODO check that the deposited token matches 'currency'
			// This will never result in an entry
			if nativeCurrency != "GBP" {
				fmt.Printf("In row %d, expected 'Native Currency' %s but found %s [%s]\n", csvRowIndex, "GBP", nativeCurrency, row)
			}
		} else if strings.Contains(description, " -> ") {
			// TODO when handling conversions, go from source to target
			// This should eventually result in an entry for the target and a negative entry for the source (unless it is a currency)
			fmt.Println("found exchange")
		} else if description == "CRO Stake" {
			// TODO When other staking is allowed, check that 'currency' matches the staked currency
			if nativeCurrency != "GBP" {
				fmt.Printf("In row %d, expected 'Native Currency' %s but found %s [%s]\n", csvRowIndex, "GBP", nativeCurrency, row)
			}
		} else if strings.HasSuffix(description, " Stake Rewards") {
			// TODO: handle staking rewards of cyptocurrencies other than CRO
			// Check the required values are as expected
			// Check the required values are as expected
			if !areRowValuesAcceptable(csvRowIndex, row, currency, "CRO", nativeCurrency, "GBP", kind, "mco_stake_reward", "", "") {
				fmt.Println("Bad value seen")
				entry := []string{"**BAD DATA**", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "STAKING **BAD DATA**"}
				output = append(output, entry)
			} else {
				entry := []string{"", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "STAKING"}
				output = append(output, entry)
			}
		} else if description == "Card Cashback" {
			// Check the required values are as expected
			if !areRowValuesAcceptable(csvRowIndex, row, currency, "CRO", nativeCurrency, "GBP", kind, "referral_card_cashback", "", "") {
				fmt.Println("Bad value seen")
				entry := []string{"***BAD DATA***", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "CASHBACK **BAD DATA**"}
				output = append(output, entry)
			} else {
				entry := []string{"", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "CASHBACK"}
				output = append(output, entry)
			}
		} else {
			fmt.Printf("UNRECOGNISED <%s>\n", description)
			entry := []string{"***UNRECOGNISED***", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "***INVALID***"}
			output = append(output, entry)
		}
	}

	// hacky code from stackoverflow to reverse the slice
	for i, j := 0, len(output)-1; i < j; i, j = i+1, j-1 {
		output[i], output[j] = output[j], output[i]
	}

	return output
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

// Converts from UTC to UK local time.

// UTC and GMT match.
// During these dates (from https://www.gov.uk/when-do-the-clocks-change) the UK runs on GMT+1:
//
// 2020 	29 March 	25 October
// 2021 	28 March 	31 October
// 2022 	27 March 	30 October
// 2023 	26 March 	29 October
//
// In practice the next BST date is in NOV-2021 so at least until 27-MAR-2022 no conversion needs to happen until then.
func convertUtcToUKTime(utcTime string) string {
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
func areRowValuesAcceptable(csvRow int, row []string, currency string, expectedCurrency string, nativeCurrency string, expectedNativeCurrency string, kind string, expectedKind string, toCurrency string, toAmount string) bool {
	ok := true
	if currency != expectedCurrency {
		fmt.Printf("In row %d, expected currency %s but found %s [%s]\n", csvRow, expectedCurrency, currency, row)
		ok = false
	}
	if nativeCurrency != expectedNativeCurrency {
		fmt.Printf("In row %d, expected 'Native Currency' %s but found %s [%s]\n", csvRow, expectedNativeCurrency, nativeCurrency, row)
		ok = false
	}
	if kind != expectedKind {
		fmt.Printf("In row %d, expected 'Kind' to be %s but found %s [%s]\n", csvRow, expectedKind, kind, row)
		ok = false
	}
	if toCurrency != "" {
		fmt.Printf("In row %d, expected 'toCurrency' to be blank but found <%s> [%s]\n", csvRow, toCurrency, row)
		ok = false
	}
	if toAmount != "" {
		fmt.Printf("In row %d, expected 'toAmount' to be blank but found <%s> [%s]\n", csvRow, toAmount, row)
		ok = false
	}
	return ok
}
