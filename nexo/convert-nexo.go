package main

// This program takes a CSV from nexo.io and adjusts it into a format suitable for my records.
// Where applicable it rearranges each row into a format that matches my exchange-independent records.

// TODO
// The usage should be explained here
// Row processing should be enhanced to check all rows, even those that produce no output.

// Notes:
// Timestamps are in CET. These are NOT YET converted to UK local time.

// Note that an older format was used until some time between 2022-03-16 and 2022-04-06.
//
// Current CSV format:
//
// Transaction: a transaction identifier
// Type: the type of transaction (see below)
// Input Currency: e.g. NEXONEXO, GBP but also e.g. GBPX/UST for a currency purchase
// Input Amount:
// Output Currency:
// Output Amount:
// USD Equivalent: USD ($) amount (presumably at the time)
// Details: always starts with "approved/"
// Outstanding Loan: always "$0.00"
// Date / Time: YYYY-MM-DD HH:MM:SS
//
// Transaction Type:
//     Interest: Represents a staking reward
//       Input and Output Currency: always NEXO
//       Details: "approved / AMOUNT GBPx", AMOUNT will be the token value at that time in GBP
//     FixedTermInterest: handled identically to "Interest" except Details is "approved / Term Deposit Interest"
//     Exchange: Essentially a purchase (or a sale) of a token or coin
//       Input Currency: "GBPX/target-currency"
//       Input Amount: "CURRENCY AMOUNT"
//       Output Currency: CURRENCY
//       Output Amount: AMOUNT
//       Details: "approved / Exchange GBPX to CURRENCY-NAME"
//     DepositToExchange: Fiat currency sent to nexo.io:
//       Input Currency: GBP
//       Output Currency: GBPX
//       Input Amount/Output Amount: must match
//       Details: "approved / GBP Top Up"
//     ExchangeDepositedOn:
//       Input Currency: GBP
//       Output Currency: GBPX
//       Input Amount/Output Amount: must match
//       Details: "approved / GBP to GBPX"
//     LockingTermDeposit: Represents a currency being moved into a Term Wallet to earn staking rewards
//       Input/Output Currency: NEXO, GBPX
//       Details: "approved / Transfer from Savings Wallet to Term Wallet"
//     Unlocking Term Deposit: Represents a currency being moved out of a Term Wallet
//       TBD
//     ExchangeToWithdraw: represents GBPX conversion to GBP prior to withdrawl
//       Input Currency: GBPX
//       Output Currency: GBP
//       Details: "approved / GBPX to GBP"
//     **WithdrawExchanged: Withdrawl of GBP to a bank account
//       Currency: GBP
//       Details: "approved / GBP Withdrawal"
//     Exchange Cashback: an airdrop
//       Input Currency: BTC
//       Output Currency: BTC
//       Input/Output Amount: must match
//       Details: "approved / 0.5% on top of your Exchange transaction"
//     **Deposit: a reward from nexo.io

// TODO
// Handle deposit: turn into TRANSFER-IN
// Handle GBP -> XXX: this is a BUY of XXX
// Handle Crypto Earn: this is STAKING
// Handle  Card Cashback Reversal (look for identical transaction later???)
// Handle Withdraw: this is TRANSFER-OUT
// Add option to spread out transactions by currency, so group together all CRO and all AVAX etc.

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func main() {

	flag.Parse()

	inputs := flag.Args()
	if len(inputs) != 2 {
		log.Fatalf("Exactly 2 arguments required but %d supplied\n", len(inputs))
	}

	transactionsFilename := flag.Arg(0)
	outputFile := flag.Arg(1)

	transactions := readTransactions(transactionsFilename)

	// Stop immediately if there are not enough rows in the input CSV
	if len(transactions) < 2 {
		log.Fatalf("Input CSV file %s must contain at least two rows and it does not. Only %d rows present.", transactionsFilename, len(transactions))
	}

	// The first element must match this exactly otherwise the format may have changed:
	expectedFirstRow := []string{"Transaction", "Type", "Input Currency", "Input Amount", "Output Currency", "Output Amount", "USD Equivalent", "Details", "Outstanding Loan", "Date / Time"}
	firstRow := transactions[0]
	if !testSlicesEqual(firstRow, expectedFirstRow) {
		fmt.Printf("Expected first row format: %s\n", expectedFirstRow)
		fmt.Printf("Actual first row format:   %s\n", firstRow)
		fmt.Printf("lengths: expected: %d, actual: %d\n", len(expectedFirstRow), len(firstRow))
		for i := 0; i < len(firstRow); i = i + 1 {
			if firstRow[i] != expectedFirstRow[i] {
				fmt.Printf("Mismatch found at element %d. Actual: [%s], expected: [%s]\n", i, firstRow[i], expectedFirstRow[i])
			} else {
				fmt.Printf("Match for element %d\n", i)
			}
		}
		log.Fatalf("First CSV row fails to match expectations. Perhaps the format has changed?")
	}

	// The first element is the identification row, which now just gets in the way
	transactions = transactions[1:]

	// The transactions in the input CSV are in reverse time order.const
	// Put them in forward time order so that they are processed chronologically.
	// (Hacky code from stackoverflow to reverse the slice)
	for i, j := 0, len(transactions)-1; i < j; i, j = i+1, j-1 {
		transactions[i], transactions[j] = transactions[j], transactions[i]
	}

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

func convertTransactions(transactions [][]string) [][]string {

	// TBD
	// This needs to record per-currency (as per kraken) using the Output Currency as the key.
	// That will group together transactions correctly.
	// Currently BUY and SELL will not be recorded correctly

	// What is the correct way of handling NEXO/USDC and USDC/UST transactions?
	// Note that a GBPX=>NEXO transaction does not record the amount of GBPX exchanged, only the dollar equivalent.

	// row[0] : Transaction
	// row[1] : Type
	// row[2] : Input Currency
	// row[3] : Input Amount
	// row[4] : Output Currency
	// row[5] : Output Amount
	// row[6] : USD Equivalent
	// row[7] : Details
	// row[8] : Outstanding Loan
	// row[9] : Date / Time

	output := make([][]string, 0)             // output (array of strings)
	exchangeToWithdraw := make([][]string, 0) // FIFO queue or records

	for _, row := range transactions {
		// So far, "Outstanding Loan" is *always* "$0.00", so check that immediately
		if row[8] != "$0.00" {
			fmt.Printf("TX %s: Outstanding Load error: %s\n", row[0], row[8])
		}

		// Handle each transaction Type separately
		switch row[1] { // row[1] is the "Type"
		case "LockingTermDeposit":
			// LockingTermDeposit represents moving a token from the normal wallet into a wallet where it earns higher STAKING rewards in return for being locked.
			// This line generates no output and is checked purely to ensure that the format is understood and has not changed.
			// Input/Output Currency must be identical
			if row[2] != row[4] {
				fmt.Printf("TX %s: LockingTermDeposit currency error: input: %s, output: %s\n", row[0], row[2], row[2])
			}
			// Input Amount and Output Amount must be identical in absolute value the former is negative and the latter is positive.
			if row[3][0] != '-' || row[3][1:] != row[5] {
				valuesDiffer := true
				if row[2] == "GBPX" {
					inputAmountFloat, err := strconv.ParseFloat(row[3], 64)
					if err != nil {
						fmt.Printf("TX %s: LockingTermDeposit Input Amount conversion error: %s, issue: %s\n", row[0], row[3], err)
					}
					outputAmountFloat, err := strconv.ParseFloat(row[5], 64)
					if err != nil {
						fmt.Printf("TX %s: LockingTermDeposit Output Amount conversion error: %s, issue: %s\n", row[0], row[5], err)
					}
					if inputAmountFloat == -outputAmountFloat {
						valuesDiffer = false
					}
				}
				if valuesDiffer {
					fmt.Printf("TX %s: LockingTermDeposit currency amount error: input: %s, output: %s\n", row[0], row[3], row[5])
				}
			}
			//       Details: "approved / Transfer from Savings Wallet to Term Wallet"
			if !strings.HasPrefix(row[7], "approved / Transfer from Savings Wallet to Term Wallet") {
				fmt.Printf("TX %s: LockingTermDeposit Details error: input: %s\n", row[0], row[7])
			}
			if row[6][0] != '$' {
				fmt.Printf("TX %s: LockingTermDeposit not in dollars [%s]\n", row[0], row[6])
			}
		case "UnlockingTermDeposit":
			// UnlockingTermDeposit represents moving a token from the long term wallet into a normal wallet at the end of a term period.
			// This line generates no output and is checked purely to ensure that the format is understood and has not changed.
			// Input/Output Currency must be identical
			if row[2] != row[4] {
				fmt.Printf("TX %s: UnlockingTermDeposit currency error: input: %s, output: %s\n", row[0], row[2], row[2])
			}
			// Input Amount and Output Amount must be identical.
			if row[3] != row[5] {
				valuesDiffer := true
				if row[2] == "GBPX" {
					inputAmountFloat, err := strconv.ParseFloat(row[3], 64)
					if err != nil {
						fmt.Printf("TX %s: UnlockingTermDeposit Input Amount conversion error: %s, issue: %s\n", row[0], row[3], err)
					}
					outputAmountFloat, err := strconv.ParseFloat(row[5], 64)
					if err != nil {
						fmt.Printf("TX %s: UnlockingTermDeposit Output Amount conversion error: %s, issue: %s\n", row[0], row[5], err)
					}
					if inputAmountFloat == outputAmountFloat {
						valuesDiffer = false
					}
				}
				if valuesDiffer {
					fmt.Printf("TX %s: UnlockingTermDeposit currency amount error: input: %s, output: %s\n", row[0], row[3], row[5])
				}
			}
			//       Details: "approved / Transfer from Term Wallet to Savings Wallet"
			if !strings.HasPrefix(row[7], "approved / Transfer from Term Wallet to Savings Wallet") {
				fmt.Printf("TX %s: UnlockingTermDeposit Details error: input: %s\n", row[0], row[7])
			}
			if row[6][0] != '$' {
				fmt.Printf("TX %s: UnlockingTermDeposit not in dollars [%s]\n", row[0], row[6])
			}

		case "FixedTermInterest":
			// "FixedTermInterest" is a staking reward that happens in a "Long Term Wallet".
			// This is handled almost identically to "Interest".
			fallthrough
		case "Interest":
			// "Interest" transactions need to be recorded as "STAKING"
			// Input/Output Currency must be NEXO
			if (row[2] != "NEXO") || (row[4] != "NEXO") {
				fmt.Printf("TX %s: Interest currency error: input: %s, output: %s\n", row[0], row[2], row[2])
			}
			// Input Amount and Output Amount must be identical
			if row[3] != row[5] {
				// TBD fmt.Printf("TX %s: Interest currency amount error: input: %s, output: %s\n", row[0], row[3], row[5])
			}
			//       Details: "approved / AMOUNT GBPx", AMOUNT will be the token value at that time in GBP
			if !strings.HasPrefix(row[7], "approved / ") {
				fmt.Printf("TX %s: Interest Details error: input: %s\n", row[0], row[7])
			}
			if row[6][0] != '$' {
				fmt.Printf("TX %s: Interest not in dollars [%s]\n", row[0], row[6])
			}
			// [3] is amount of nexo
			// [6] is USD earned (but the "$" needs to be stripped)
			// [9] is date/time in CET
			// Output should be "nexo.io", date/time, uk date/time, nexo, (price), total, exch, £, "", "", "", "", "STAKING"
			// Double check that the "USD equivalent" is stated in USD
			entry := []string{"", "nexo.io", row[9], "", row[3], "", row[6][1:], "", "", "", "", "", "", "STAKING"}
			output = append(output, entry)
		case "Deposit":
			// "Deposit" transactions need to be recorded as "REWARD"

			// Input/Output Currency must be NEXO
			if (row[2] != "NEXO") || (row[4] != "NEXO") {
				fmt.Printf("TX %s: Deposit currency error: input: %s, output: %s\n", row[0], row[2], row[2])
			}
			// Input Amount and Output Amount must be identical
			if row[3] != row[5] {
				// TBD fmt.Printf("TX %s: Interest currency amount error: input: %s, output: %s\n", row[0], row[3], row[5])
			}
			// Details: "approved / Nexonomics Exchange Cash-back Promotion"
			if row[7] != "approved / Nexonomics Exchange Cash-back Promotion" {
				fmt.Printf("TX %s: Deposit Details error: input: %s\n", row[0], row[7])
			}
			// Double check that the "USD equivalent" is stated in USD
			if row[6][0] != '$' {
				fmt.Printf("TX %s: Deposit not in dollars [%s]\n", row[0], row[6])
			}
			// [3] is amount of nexo
			// [6] is USD earned (but the "$" needs to be stripped)
			// [9] is date/time in CET
			// Output should be "nexo.io", date/time, uk date/time, nexo, (price), total, exch, £, "", "", "", "", "STAKING"
			entry := []string{"", "nexo.io", row[9], "", row[3], "", row[6][1:], "", "", "", "", "", "", "REWARD"}
			output = append(output, entry)
		case "Exchange Cashback":
			// Input/Output Currency must be BTC (because that is the only example so far)
			if (row[2] != "BTC") || (row[4] != "BTC") {
				fmt.Printf("TX %s: Exchange currency error: input: %s, output: %s\n", row[0], row[2], row[2])
			}
			// Input Amount and Output Amount must be identical
			if row[3] != row[5] {
				fmt.Printf("TX %s: Exchange currency amount error: input: %s, output: %s\n", row[0], row[3], row[5])
			}
			// Details: "approved / 0.5% on top of your Exchange transaction"
			if row[7] != "approved / 0.5% on top of your Exchange transaction" {
				fmt.Printf("TX %s: Exchange Details error: input: %s\n", row[0], row[7])
			}
			// Double check that the "USD equivalent" is stated in USD
			if row[6][0] != '$' {
				fmt.Printf("TX %s: Exchange not in dollars [%s]\n", row[0], row[6])
			}
			// TBD
			// Nothing yet recorded because I do not know how to record it!
		case "Exchange":
			// "Exchange" transactions represent a purchase and need to be recorded as "BUY"
			// TBD: "GBPX/token" is a purchase of that token
			// TBD: "tokenA/tokenB" is a SELL of tokenA followed by a BUY of tokenB. both prices are in $
			// TBD: split row[2] at "tokenA/tokenB"
			// TBD: if tokenA is GBPX, treat as a BUY of row[5] units of tokenB at row[6] USD
			// TBD: otherwise treat as sale of tokenA for tokenB; unfortunately amount of tokenA is not available!!
			// The Output Currency must be one of BTC, NEXO, USDC, UST
			allowedExchangeCurrency := map[string]bool{
				"BTC":  true,
				"NEXO": true,
				"USDC": true,
				"UST":  true,
			}
			if !allowedExchangeCurrency[row[4]] {
				fmt.Printf("TX %s: Exchange output currency error: %s\n", row[0], row[4])
			}
			// Input Currency must be GBPX/???? where ???? is the Output Currency
			expectedInputCurrency := "GBPX/" + row[4]
			if row[2] != expectedInputCurrency {
				fmt.Printf("TX %s: Exchange input currency error: expected: %s, actual: %s\n", row[0], expectedInputCurrency, row[2])
			}
			// Input Amount is the text of Output Currency followed by Output Amount
			expectedInputAmount := row[4] + " " + row[5]
			if row[3] != expectedInputAmount {
				fmt.Printf("TX %s: Exchange input amount error: expected: %s, actual: %s\n", row[0], expectedInputAmount, row[3])
			}
			// Details: "approved / Nexonomics Exchange Cash-back Promotion"
			// TDB
			//if row[7] != "approved / Nexonomics Exchange Cash-back Promotion" {
			//	fmt.Printf("TX %s: Deposit Details error: input: %s\n", row[0], row[7])
			//}
			// Double check that the "USD equivalent" is stated in USD
			if row[6][0] != '$' {
				fmt.Printf("TX %s: Deposit not in dollars [%s]\n", row[0], row[6])
			}
			// Output should be "nexo.io", date/time, uk date/time, nexo, (price), total, exch, £, "", "", "", "", "STAKING"
			// entry := []string{"", "nexo.io", row[9], "", row[3], "", row[6][1:], "", "", "", "", "", "", "BUY"}
			// TBD - list once things are separated by currency
			//// output = append(output, entry)
			//// fmt.Printf("NOT outputting %s: %s\n", row[1], entry)
		case "ExchangeToWithdraw":
			// ExchangeToWithDraw represents the first of two operations that are involved in removing funds from NEXO.
			// This transaction records a 1:1 converion of GBPX to GBP.
			// There should be a correspodning (later) matching WithdrawExchanged that records the actual removal of the funds.
			// For now it is assumed that the corresponding WithdrawExchanged records occur in the same order as the corresponding
			// ExchangeToWithdraw records so that all that is needed to match is a simple FIFO.

			// "Input Currency" will always be GBPX and "Output Currency" will always be GBP
			if (row[2] != "GBPX") || (row[4] != "GBP") {
				fmt.Printf("TX %s: ExchangeToWithdraw does not use GBP [%s,%s]\n", row[0], row[2], row[4])
			}
			// Input Amount and Output Amount must be identical in absolute value the former is negative and the latter is positive.
			if row[3][0] != '-' || row[3][1:] != row[5] {
				valuesDiffer := true
				if row[2] == "GBPX" {
					inputAmountFloat, err := strconv.ParseFloat(row[3], 64)
					if err != nil {
						fmt.Printf("TX %s: ExchangeToWithdraw Input Amount conversion error: %s, issue: %s\n", row[0], row[3], err)
					}
					outputAmountFloat, err := strconv.ParseFloat(row[5], 64)
					if err != nil {
						fmt.Printf("TX %s: ExchangeToWithdraw Output Amount conversion error: %s, issue: %s\n", row[0], row[5], err)
					}
					if inputAmountFloat == -outputAmountFloat {
						valuesDiffer = false
					}
				}
				if valuesDiffer {
					fmt.Printf("TX %s: ExchangeToWithdraw currency amount error: input: %s, output: %s\n", row[0], row[3], row[5])
				}
			}
			// [6] will be the dollar equivalent (just check that it starts '$)
			if row[6][0] != '$' {
				fmt.Printf("TX %s: ExchangeToWithdraw dollar equivalent invalid [%s]\n", row[0], row[6])
			}
			// [7] will be "approved / GBPX to GBP"
			if row[7] != "approved / GBPX to GBP" {
				fmt.Printf("TX %s: ExchangeToWithdraw details invalid [%s]\n", row[0], row[7])
			}
			exchangeToWithdraw = append(exchangeToWithdraw, row) // Add the record to the FIFO
		case "WithdrawExchanged":
			// WithdrawExchanged represents the second of two operations that are involved in removing funds from NEXO.
			// This transaction records the actual withdrawal of GBP from NEXO.
			// There should be a corresponding (earlier) matching ExchangeToWithdraw.
			// [2] will always be GBPX
			// [3] will be a negative amount and [5] will be the corresponding positive amount
			// [4] will always be GBP
			// [6] will be the dollar equivalent (just check that it starts '$)
			// [7] will be "approved / GBPX to GBP"
			// [9] is date/time in CET
			if (row[2] != "GBP") || (row[4] != "GBP") {
				fmt.Printf("TX %s: ExchangeToWithdraw does not use GBP [%s,%s]\n", row[0], row[2], row[4])
			}
			//if (row[3] >= -22) || (row[3] != -row[5]) {
			//	fmt.Printf("TX %s: ExchangeToWithdraw amount inconsistent [%s,%s]\n", row[0], row[3], row[5])
			//}
			if row[6][0] != '$' {
				fmt.Printf("TX %s: ExchangeToWithdraw dollar equivalent invalid [%s]\n", row[0], row[6])
			}
			if row[7] != "approved / GBP withdrawal" {
				fmt.Printf("TX %s: ExchangeToWithdraw details invalid [%s]\n", row[0], row[7])
			}
			if len(exchangeToWithdraw) < 0 {
				fmt.Printf("TX %s: WithdrawExchanged with no matching ExchangeToWithdraw\n", row[0])
			} else {
				matchingExchangeToWithdraw := exchangeToWithdraw[0] // Get the presumed matching record
				exchangeToWithdraw = exchangeToWithdraw[1:]         // Remove that record from the FIFO
				// Both this record and the presumed matching ExchangeToWithdraw have been checked for validity.
				// To check for a match asll that is needed is that "Input Amount" [3], "Output Currency" [4]
				// Note that "USD Equivalent" may not match presumably because the £/$ exchange rate may drift slightly
				// between the times when the ExchangeToWithdraw and the WithdrawExchanged happen.
				if (row[3] != matchingExchangeToWithdraw[3]) || (row[4] != matchingExchangeToWithdraw[4]) {
					fmt.Printf("TX %s: WithdrawExchanged finds non-matching ExchangeToWithdraw [TX: %s]\n", row[0], matchingExchangeToWithdraw[0])
				}
			}
			// Nothing needs to be recorded for a removal of fiat from NEXO
		case "DepositToExchange":
			fmt.Printf("Unhandled switch option:[%s]\n", row[1])
		case "ExchangeDepositedOn":
			fmt.Printf("Unhandled switch option:[%s]\n", row[1])
		default:
			fmt.Printf("Unhandled switch option:[%s]\n", row[1])
		}
	}

	// At this point the exchangeToWithdraw FIFO should be empty
	if len(exchangeToWithdraw) > 0 {
		fmt.Printf("There are ")
	}

	return output
}

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
