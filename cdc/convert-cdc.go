package main

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
	"strings"
	"time"
)

// Open the input file and convert it to the output format
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
	expectedFirstRow := []string{"Timestamp (UTC)", "Transaction Description", "Currency", "Amount", "To Currency", "To Amount", "Native Currency", "Native Amount", "Native Amount (in USD)", "Transaction Kind", "Transaction Hash"}

	firstRow := transactions[0]

	if !testSlicesEqual(firstRow, expectedFirstRow) {
		log.Fatalf("First row fails to match")
	}

	output := make(map[string][][]string, 0)

	for i, row := range transactions[1:] {
		csvRowIndex := i + 2
		exchangeTime := row[0]
		description := row[1]
		currency := row[2]
		amount := row[3]
		toCurrency := row[4]
		toAmount := row[5]
		nativeCurrency := row[6]
		nativeAmount := row[7]
		//nativeAmountUSD := row[8]
		kind := row[9]

		ukTime := convertUtcToUKTime(exchangeTime)

		if description == "Sign-up Bonus Unlocked" {
			// This entry refers to a reward given for using someone's signup code or given when someone else uses your signup code.
			// Check the required values are as expected
			if !areRowValuesAcceptable(csvRowIndex, row, currency, "CRO", nativeCurrency, "USD", kind, "referral_gift", "", "") {
				fmt.Println("Bad value seen (Sign-up Bonus Unlocked)")
				entry := []string{"**BAD DATA**", "crypto.com App", exchangeTime, ukTime, amount, "", nativeAmount, "", "", "", "", "", "", "REWARD **BAD DATA**"}
				output[currency] = append(output[currency], entry)
			} else {
				entry := []string{"", "crypto.com App", exchangeTime, ukTime, amount, "", nativeAmount, "", "", "", "", "", "", "REWARD"}
				output[currency] = append(output[currency], entry)
			}
		} else if description == "Crypto Earn Deposit" {
			// TODO this just indicates that crypto has been deposited into the Crypto Earn wallet.
			// No entry is written.
			// This must be before the code that checks for a suffix of "Deposit", otherwise the code will not interpret the row properly
		} else if description == "Crypto Earn Withdrawal" {
			// This is a withdrawal of a cryptocurrency from the Earn program.
			// There are no tax implications, so verify the data but do nothing else.

			// Check the required values are as expected
			if !areRowValuesAcceptable(csvRowIndex, row, currency, currency, nativeCurrency, "GBP", kind, "crypto_earn_program_withdrawn", "", "") {
				fmt.Println("Bad value seen (Crypto Earn Withdrawal)")
				entry := []string{"***BAD DATA***", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "REWARD **BAD DATA**"}
				output[currency] = append(output[currency], entry)
			}
		} else if strings.HasSuffix(description, " Deposit") {
			// TODO check that the deposited token matches 'currency'
			// This will result in a TRANSFER-IN entry
			fields := strings.Fields(description)
			depositCurrency := fields[0]
			if !areRowValuesAcceptable(csvRowIndex, row, currency, depositCurrency, nativeCurrency, "GBP", kind, "crypto_deposit", "", "") {
				fmt.Println("Bad value seen (* Deposit)")
				entry := []string{"**BAD DATA**", "crypto.com App", exchangeTime, ukTime, amount, "", nativeAmount, "", "", "", "", "", "", "TRANSFER-IN **BAD DATA**"}
				output[currency] = append(output[currency], entry)
			} else {
				entry := []string{"", "crypto.com App", exchangeTime, ukTime, amount, "", nativeAmount, "", "", "", "", "", "", "TRANSFER-IN"}
				output[currency] = append(output[currency], entry)
			}
		} else if strings.Contains(description, " -> ") {
			// This must be after the code that checks for "Crypto Earn Deposit", as this code will not interpret that row properly.
			// If the trasaction kind is "viban_purchase", this entry represents a swap from currency or fiat to a cryptocurrency.
			// If the trasaction kind is "crypto_exchange", this entry represents a swap from one cryptocurrency to another (e.g. USDC to BTC).
			// Currently all such entries are from GBP to another currency and represent a simple purchase, so the checks are hard-coded to expect GBP.
			fields := strings.Fields(description)
			convertFromCurrency := fields[0]
			convertToCurrency := fields[2]
			if kind == "viban_purchase" {
				if !areRowValuesAcceptable(csvRowIndex, row, currency, "GBP", nativeCurrency, convertFromCurrency, kind, "viban_purchase", "", "") || (currency != convertFromCurrency) || (toCurrency != convertToCurrency) {
					fmt.Println("Bad value seen (-> exchange [viban_purchase])")
					entry := []string{"**BAD DATA**", "crypto.com App", exchangeTime, ukTime, toAmount, "", "", "", nativeAmount, "", "", "", "", "BUY **BAD DATA**"}
					output[currency] = append(output[convertToCurrency], entry)
				} else {
					entry := []string{"", "crypto.com App", exchangeTime, ukTime, toAmount, "", "", "", nativeAmount, "", "", "", "", "BUY"}
					output[convertToCurrency] = append(output[convertToCurrency], entry)
				}
			} else if kind == "crypto_exchange" {
				if !areRowValuesAcceptable(csvRowIndex, row, currency, convertFromCurrency, nativeCurrency, "GBP", kind, "crypto_exchange", "", "") || (currency != convertFromCurrency) || (toCurrency != convertToCurrency) {
					fmt.Println("Bad value seen (-> exchange [crypto_exchange])")
					entry := []string{"**BAD DATA**", "crypto.com App", exchangeTime, ukTime, toAmount, "", "", "", nativeAmount, "", "", "", "", "BUY **BAD DATA**"}
					output[currency] = append(output[convertToCurrency], entry)
				} else {
					fmt.Printf("Debug: date: %s  %s (%s)->%s(%s)\n", ukTime, convertFromCurrency, amount, convertToCurrency, toAmount)
					// This is a SELL of "amount" of "convertFromCurrency" ...
					entry := []string{"", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "SELL"}
					output[convertFromCurrency] = append(output[convertFromCurrency], entry)
					// ... followed by a BUY of "toAmount" of "convertToCurrency"
					entry = []string{"", "crypto.com App", exchangeTime, ukTime, toAmount, "", "", "", nativeAmount, "", "", "", "", "BUY"}
					output[convertToCurrency] = append(output[convertToCurrency], entry)
				}
			} else {
				fmt.Println("Bad value seen (-> exchange [UNKNOWN])")
				entry := []string{"**BAD DATA**", "crypto.com App", exchangeTime, ukTime, toAmount, "", "", "", nativeAmount, "", "", "", "", "BUY **BAD DATA**"}
				output[currency] = append(output[convertToCurrency], entry)

			}
		} else if description == "CRO Stake Rewards" {
			// crypto.com lists CRO staking rewards in a special way. This entry represnts a staking reward given on the CRO that has been staked for the VISA pre-paid card.
			// Check the required values are as expected
			if !areRowValuesAcceptable(csvRowIndex, row, currency, "CRO", nativeCurrency, "GBP", kind, "mco_stake_reward", "", "") {
				fmt.Println("Bad value seen (CRO Stake Rewards)")
				entry := []string{"**BAD DATA**", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "STAKING **BAD DATA**"}
				output[currency] = append(output[currency], entry)
			} else {
				entry := []string{"", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "STAKING"}
				output[currency] = append(output[currency], entry)
			}
		} else if description == "CRO Stake" {
			// This entry indicates that CRO has been staked (rather than depostied into the Crypto Earn programme)
			// No entry is written: this code is here just to check that the data is as expected.
			// Check the required values are as expected
			if !areRowValuesAcceptable(csvRowIndex, row, currency, "CRO", nativeCurrency, "GBP", kind, "lockup_lock", "", "") {
				fmt.Println("Bad value seen (CRO Stake)")
				entry := []string{"**BAD DATA**", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "CRO STAKE **BAD DATA**"}
				output[currency] = append(output[currency], entry)
			}
		} else if description == "CRO Unstake" {
			// This entry indicates that CRO has been unstaked (rather than removed from the Crypto Earn programme)
			// No entry is written: this code is here just to check that the data is as expected.
			// Check the required values are as expected
			if !areRowValuesAcceptable(csvRowIndex, row, currency, "CRO", nativeCurrency, "GBP", kind, "lockup_unlock", "", "") {
				fmt.Println("Bad value seen (CRO Unstake)")
				entry := []string{"**BAD DATA**", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "CRO UNSTAKE **BAD DATA**"}
				output[currency] = append(output[currency], entry)
			}
		} else if description == "Card Cashback" {
			// This entry represents CRO paid as cashback for purchases made on the VISA card.
			// Check the required values are as expected
			if !areRowValuesAcceptable(csvRowIndex, row, currency, "CRO", nativeCurrency, "GBP", kind, "referral_card_cashback", "", "") {
				fmt.Println("Bad value seen (Card Cashback)")
				entry := []string{"***BAD DATA***", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "CASHBACK **BAD DATA**"}
				output[currency] = append(output[currency], entry)
			} else {
				entry := []string{"", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "CASHBACK"}
				output[currency] = append(output[currency], entry)
			}
		} else if description == "Crypto Earn" {
			// This entry represents a currency earned through staking.
			// TODO: need to find out the currency that has been earned (this is 'currency')
			entry := []string{"", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "STAKING"}
			output[currency] = append(output[currency], entry)
		} else if description == "Card Cashback Reversal" {
			// This entry represents a VISA card cashback that has been reversed.
			// Check the required values are as expected
			if !areRowValuesAcceptable(csvRowIndex, row, currency, "CRO", nativeCurrency, "GBP", kind, "card_cashback_reverted", "", "") {
				fmt.Println("Bad value seen (Card Cashback Reversal)")
				entry := []string{"***BAD DATA***", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "CASHBACK-REVERSAL **BAD DATA**"}
				output[currency] = append(output[currency], entry)
			} else {
				entry := []string{"", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "CASHBACK-REVERSAL"}
				output[currency] = append(output[currency], entry)
			}
		} else if strings.HasPrefix(description, "Withdraw ") {
			// This represents the transfer of a cyrptocurrency to another wallet
			fields := strings.Fields(description)
			withdrawCurrency := fields[1]

			// Check the required values are as expected
			if !areRowValuesAcceptable(csvRowIndex, row, currency, withdrawCurrency, nativeCurrency, "GBP", kind, "crypto_withdrawal", "", "") {
				fmt.Println("Bad value seen (Withdraw *)")
				entry := []string{"***BAD DATA***", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "TRANSFER-OUT **BAD DATA**"}
				output[currency] = append(output[currency], entry)
			} else {
				entry := []string{"", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "TRANSFER-OUT"}
				output[currency] = append(output[currency], entry)
			}
		} else if strings.HasPrefix(description, "To +") {
			// This is a transfer of a token to another crypto.com user

			// Check the required values are as expected
			if !areRowValuesAcceptable(csvRowIndex, row, currency, "CRO", nativeCurrency, "GBP", kind, "crypto_transfer", "", "") {
				fmt.Println("Bad value seen (To +)")
				entry := []string{"***BAD DATA***", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "TRANSFER-OUT **BAD DATA**"}
				output[currency] = append(output[currency], entry)
			} else {
				entry := []string{"", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "TRANSFER-OUT"}
				output[currency] = append(output[currency], entry)
			}
		} else if strings.HasPrefix(description, "From +") {
			// This is a transfer of a token to another crypto.com user

			// Check the required values are as expected
			if !areRowValuesAcceptable(csvRowIndex, row, currency, "CRO", nativeCurrency, "GBP", kind, "crypto_transfer", "", "") {
				fmt.Println("Bad value seen (From +)")
				entry := []string{"***BAD DATA***", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "TRANSFER-IN **BAD DATA**"}
				output[currency] = append(output[currency], entry)
			} else {
				entry := []string{"", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "TRANSFER-IN"}
				output[currency] = append(output[currency], entry)
			}
		} else if description == "Pay Rewards" {
			// This is a transfer of a token to another crypto.com user

			// Check the required values are as expected
			if !areRowValuesAcceptable(csvRowIndex, row, currency, "CRO", nativeCurrency, "GBP", kind, "transfer_cashback", "", "") {
				fmt.Println("Bad value seen (Pay Rewards)")
				entry := []string{"***BAD DATA***", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "REWARD **BAD DATA**"}
				output[currency] = append(output[currency], entry)
			} else {
				entry := []string{"", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "REWARD"}
				output[currency] = append(output[currency], entry)
			}
		} else {
			fmt.Printf("UNRECOGNISED <%s>\n", description)
			entry := []string{"***UNRECOGNISED***", "crypto.com App", exchangeTime, ukTime, amount, "", "", "", nativeAmount, "", "", "", "", "***INVALID***"}
			output[currency] = append(output[currency], entry)
		}
	}

	// Find all the currencies in the map
	currencies := make([]string, 0)
	for k := range output {
		currencies = append(currencies, k)
	}
	sort.Strings(currencies)

	// Loop through currencies and produce an output that contains all the data categorised by currency
	finalOutput := make([][]string, 0)

	for _, c := range currencies {
		data := output[c]
		// hacky code from stackoverflow to reverse the slice
		for i, j := 0, len(data)-1; i < j; i, j = i+1, j-1 {
			data[i], data[j] = data[j], data[i]
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
	//	nextBST := time.Date(2022, 3, 27, 1, 0, 0, 0, time.UTC)
	//	if t.After(nextBST) {
	//		t = t.Add(time.Hour * 1)
	//		log.Fatalf("Adjust code to handle incursion into 2022 BST")
	//	}
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
