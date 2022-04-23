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

	// row[tx_ID] : Transaction
	// row[1] : Type
	// row[tx_InputCurrency] : Input Currency
	// row[tx_InputAmount] : Input Amount
	// row[tx_OutputCurrency] : Output Currency
	// row[tx_OutputAmount] : Output Amount
	// row[tx_UsdEquivalent] : USD Equivalent
	// row[tx_Details] : Details
	// row[tx_OutstandingLoan] : Outstanding Loan
	// row[tx_DateTime] : Date / Time
	const ( // iota is reset to 0
		tx_ID              = 0 // transaction ID
		tx_Type            = 1 //
		tx_InputCurrency   = 2 //
		tx_InputAmount     = 3 //
		tx_OutputCurrency  = 4 //
		tx_OutputAmount    = 5 //
		tx_UsdEquivalent   = 6 //
		tx_Details         = 7 //
		tx_OutstandingLoan = 8 //
		tx_DateTime        = 9 //
	)
	output := make([][]string, 0)             // output (array of strings)
	exchangeToWithdraw := make([][]string, 0) // FIFO queue or records
	depositToExchange := make([][]string, 0)  // FIFO queue or records

	for _, row := range transactions {
		// So far, "Outstanding Loan" is *always* "$0.00", so check that immediately
		if row[tx_OutstandingLoan] != "$0.00" {
			fmt.Printf("TX %s: Outstanding Load error: %s\n", row[tx_ID], row[tx_OutstandingLoan])
		}

		// Handle each transaction Type separately
		switch row[tx_Type] { // row[1] is the "Type"
		case "LockingTermDeposit":
			// LockingTermDeposit represents moving a token from the normal wallet into a wallet where it earns higher STAKING rewards in return for being locked.
			// This line generates no output and is checked purely to ensure that the format is understood and has not changed.
			// Input/Output Currency must be identical
			if row[tx_InputCurrency] != row[tx_OutputCurrency] {
				fmt.Printf("TX %s: LockingTermDeposit currency error: input: %s, output: %s\n", row[tx_ID], row[tx_InputCurrency], row[tx_OutputCurrency])
			}
			// Input Amount and Output Amount must be identical in absolute value the former is negative and the latter is positive.
			if row[tx_InputAmount][0] != '-' || row[tx_InputAmount][1:] != row[tx_OutputAmount] {
				valuesDiffer := true
				if row[tx_InputCurrency] == "GBPX" {
					inputAmountFloat, err := strconv.ParseFloat(row[tx_InputAmount], 64)
					if err != nil {
						fmt.Printf("TX %s: LockingTermDeposit Input Amount conversion error: %s, issue: %s\n", row[tx_ID], row[tx_InputAmount], err)
					}
					outputAmountFloat, err := strconv.ParseFloat(row[tx_OutputAmount], 64)
					if err != nil {
						fmt.Printf("TX %s: LockingTermDeposit Output Amount conversion error: %s, issue: %s\n", row[tx_ID], row[tx_OutputAmount], err)
					}
					if inputAmountFloat == -outputAmountFloat {
						valuesDiffer = false
					}
				}
				if valuesDiffer {
					fmt.Printf("TX %s: LockingTermDeposit currency amount error: input: %s, output: %s\n", row[tx_ID], row[tx_InputAmount], row[tx_OutputAmount])
				}
			}
			//       Details: "approved / Transfer from Savings Wallet to Term Wallet"
			if !strings.HasPrefix(row[tx_Details], "approved / Transfer from Savings Wallet to Term Wallet") {
				fmt.Printf("TX %s: LockingTermDeposit Details error: input: %s\n", row[tx_ID], row[tx_Details])
			}
			if row[tx_UsdEquivalent][0] != '$' {
				fmt.Printf("TX %s: LockingTermDeposit not in dollars [%s]\n", row[tx_ID], row[tx_UsdEquivalent])
			}
		case "UnlockingTermDeposit":
			// UnlockingTermDeposit represents moving a token from the long term wallet into a normal wallet at the end of a term period.
			// This line generates no output and is checked purely to ensure that the format is understood and has not changed.
			// Input/Output Currency must be identical
			if row[tx_InputCurrency] != row[tx_OutputCurrency] {
				fmt.Printf("TX %s: UnlockingTermDeposit currency error: input: %s, output: %s\n", row[tx_ID], row[tx_InputCurrency], row[tx_InputCurrency])
			}
			// Input Amount and Output Amount must be identical.
			if row[tx_InputAmount] != row[tx_OutputAmount] {
				valuesDiffer := true
				if row[tx_InputCurrency] == "GBPX" {
					inputAmountFloat, err := strconv.ParseFloat(row[tx_InputAmount], 64)
					if err != nil {
						fmt.Printf("TX %s: UnlockingTermDeposit Input Amount conversion error: %s, issue: %s\n", row[tx_ID], row[tx_InputAmount], err)
					}
					outputAmountFloat, err := strconv.ParseFloat(row[tx_OutputAmount], 64)
					if err != nil {
						fmt.Printf("TX %s: UnlockingTermDeposit Output Amount conversion error: %s, issue: %s\n", row[tx_ID], row[tx_OutputAmount], err)
					}
					if inputAmountFloat == outputAmountFloat {
						valuesDiffer = false
					}
				}
				if valuesDiffer {
					fmt.Printf("TX %s: UnlockingTermDeposit currency amount error: input: %s, output: %s\n", row[tx_ID], row[tx_InputAmount], row[tx_OutputAmount])
				}
			}
			//       Details: "approved / Transfer from Term Wallet to Savings Wallet"
			if !strings.HasPrefix(row[tx_Details], "approved / Transfer from Term Wallet to Savings Wallet") {
				fmt.Printf("TX %s: UnlockingTermDeposit Details error: input: %s\n", row[tx_ID], row[tx_Details])
			}
			if row[tx_UsdEquivalent][0] != '$' {
				fmt.Printf("TX %s: UnlockingTermDeposit not in dollars [%s]\n", row[tx_ID], row[tx_UsdEquivalent])
			}

		case "FixedTermInterest":
			// "FixedTermInterest" is a staking reward that happens in a "Long Term Wallet".
			// This is handled almost identically to "Interest".
			fallthrough
		case "Interest":
			// "Interest" transactions need to be recorded as "STAKING"
			// Input/Output Currency must be NEXO
			if (row[tx_InputCurrency] != "NEXO") || (row[tx_OutputCurrency] != "NEXO") {
				fmt.Printf("TX %s: Interest currency error: input: %s, output: %s\n", row[tx_ID], row[tx_InputCurrency], row[tx_InputCurrency])
			}
			// Input Amount and Output Amount must be identical
			if row[tx_InputAmount] != row[tx_OutputAmount] {
				// TBD fmt.Printf("TX %s: Interest currency amount error: input: %s, output: %s\n", row[tx_ID], row[tx_InputAmount], row[tx_OutputAmount])
			}
			//       Details: "approved / AMOUNT GBPx", AMOUNT will be the token value at that time in GBP
			if !strings.HasPrefix(row[tx_Details], "approved / ") {
				fmt.Printf("TX %s: Interest Details error: input: %s\n", row[tx_ID], row[tx_Details])
			}
			if row[tx_UsdEquivalent][0] != '$' {
				fmt.Printf("TX %s: Interest not in dollars [%s]\n", row[tx_ID], row[tx_UsdEquivalent])
			}
			// [3] is amount of nexo
			// [6] is USD earned (but the "$" needs to be stripped)
			// [9] is date/time in CET
			// Output should be "nexo.io", date/time, uk date/time, nexo, (price), total, exch, £, "", "", "", "", "STAKING"
			// Double check that the "USD equivalent" is stated in USD
			entry := []string{"", "nexo.io", row[tx_DateTime], "", row[tx_InputAmount], "", row[tx_UsdEquivalent][1:], "", "", "", "", "", "", "STAKING"}
			output = append(output, entry)
		case "Deposit":
			// "Deposit" transactions need to be recorded as "REWARD"

			// Input/Output Currency must be NEXO
			if (row[tx_InputCurrency] != "NEXO") || (row[tx_OutputCurrency] != "NEXO") {
				fmt.Printf("TX %s: Deposit currency error: input: %s, output: %s\n", row[tx_ID], row[tx_InputCurrency], row[tx_InputCurrency])
			}
			// Input Amount and Output Amount must be identical
			if row[tx_InputAmount] != row[tx_OutputAmount] {
				// TBD fmt.Printf("TX %s: Interest currency amount error: input: %s, output: %s\n", row[tx_ID], row[tx_InputAmount], row[tx_OutputAmount])
			}
			// Details: "approved / Nexonomics Exchange Cash-back Promotion"
			if row[tx_Details] != "approved / Nexonomics Exchange Cash-back Promotion" {
				fmt.Printf("TX %s: Deposit Details error: input: %s\n", row[tx_ID], row[tx_Details])
			}
			// Double check that the "USD equivalent" is stated in USD
			if row[tx_UsdEquivalent][0] != '$' {
				fmt.Printf("TX %s: Deposit not in dollars [%s]\n", row[tx_ID], row[tx_UsdEquivalent])
			}
			// [3] is amount of nexo
			// [6] is USD earned (but the "$" needs to be stripped)
			// [9] is date/time in CET
			// Output should be "nexo.io", date/time, uk date/time, nexo, (price), total, exch, £, "", "", "", "", "STAKING"
			entry := []string{"", "nexo.io", row[tx_DateTime], "", row[tx_InputAmount], "", row[tx_UsdEquivalent][1:], "", "", "", "", "", "", "REWARD"}
			output = append(output, entry)
		case "Exchange Cashback":
			// Input/Output Currency must be BTC (because that is the only example so far)
			if (row[tx_InputCurrency] != "BTC") || (row[tx_OutputCurrency] != "BTC") {
				fmt.Printf("TX %s: Exchange currency error: input: %s, output: %s\n", row[tx_ID], row[tx_InputCurrency], row[tx_InputCurrency])
			}
			// Input Amount and Output Amount must be identical
			if row[tx_InputAmount] != row[tx_OutputAmount] {
				fmt.Printf("TX %s: Exchange currency amount error: input: %s, output: %s\n", row[tx_ID], row[tx_InputAmount], row[tx_OutputAmount])
			}
			// Details: "approved / 0.5% on top of your Exchange transaction"
			if row[tx_Details] != "approved / 0.5% on top of your Exchange transaction" {
				fmt.Printf("TX %s: Exchange Details error: input: %s\n", row[tx_ID], row[tx_Details])
			}
			// Double check that the "USD equivalent" is stated in USD
			if row[tx_UsdEquivalent][0] != '$' {
				fmt.Printf("TX %s: Exchange not in dollars [%s]\n", row[tx_ID], row[tx_UsdEquivalent])
			}
			// TBD
			// Nothing yet recorded because I do not know how to record it!
		case "Exchange":
			// "Exchange" transactions represent a purchase and need to be recorded as "BUY"
			// TBD: "GBPX/token" is a purchase of that token
			// TBD: "tokenA/tokenB" is a SELL of tokenA followed by a BUY of tokenB. both prices are in $
			// TBD: split row[tx_InputCurrency] at "tokenA/tokenB"
			// TBD: if tokenA is GBPX, treat as a BUY of row[tx_OutputAmount] units of tokenB at row[tx_UsdEquivalent] USD
			// TBD: otherwise treat as sale of tokenA for tokenB; unfortunately amount of tokenA is not available!!
			// The Output Currency must be one of BTC, NEXO, USDC, UST
			allowedExchangeCurrency := map[string]bool{
				"BTC":  true,
				"NEXO": true,
				"USDC": true,
				"UST":  true,
			}
			if !allowedExchangeCurrency[row[tx_OutputCurrency]] {
				fmt.Printf("TX %s: Exchange output currency error: %s\n", row[tx_ID], row[tx_OutputCurrency])
			}
			// Input Currency must be GBPX/???? where ???? is the Output Currency
			expectedInputCurrency := "GBPX/" + row[tx_OutputCurrency]
			if row[tx_InputCurrency] != expectedInputCurrency {
				fmt.Printf("TX %s: Exchange input currency error: expected: %s, actual: %s\n", row[tx_ID], expectedInputCurrency, row[tx_InputCurrency])
			}
			// Input Amount is the text of Output Currency followed by Output Amount
			expectedInputAmount := row[tx_OutputCurrency] + " " + row[tx_OutputAmount]
			if row[tx_InputAmount] != expectedInputAmount {
				fmt.Printf("TX %s: Exchange input amount error: expected: %s, actual: %s\n", row[tx_ID], expectedInputAmount, row[tx_InputAmount])
			}
			// Details: "approved / Nexonomics Exchange Cash-back Promotion"
			// TDB
			//if row[tx_Details] != "approved / Nexonomics Exchange Cash-back Promotion" {
			//	fmt.Printf("TX %s: Deposit Details error: input: %s\n", row[tx_ID], row[tx_Details])
			//}
			// Double check that the "USD equivalent" is stated in USD
			if row[tx_UsdEquivalent][0] != '$' {
				fmt.Printf("TX %s: Deposit not in dollars [%s]\n", row[tx_ID], row[tx_UsdEquivalent])
			}
			// Output should be "nexo.io", date/time, uk date/time, nexo, (price), total, exch, £, "", "", "", "", "STAKING"
			// entry := []string{"", "nexo.io", row[tx_DateTime], "", row[tx_InputAmount], "", row[tx_UsdEquivalent][1:], "", "", "", "", "", "", "BUY"}
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
			if (row[tx_InputCurrency] != "GBPX") || (row[tx_OutputCurrency] != "GBP") {
				fmt.Printf("TX %s: ExchangeToWithdraw does not use GBP [%s,%s]\n", row[tx_ID], row[tx_InputCurrency], row[tx_OutputCurrency])
			}
			// Input Amount and Output Amount must be identical in absolute value the former is negative and the latter is positive.
			if row[tx_InputAmount][0] != '-' || row[tx_InputAmount][1:] != row[tx_OutputAmount] {
				valuesDiffer := true
				if row[tx_InputCurrency] == "GBPX" {
					inputAmountFloat, err := strconv.ParseFloat(row[tx_InputAmount], 64)
					if err != nil {
						fmt.Printf("TX %s: ExchangeToWithdraw Input Amount conversion error: %s, issue: %s\n", row[tx_ID], row[tx_InputAmount], err)
					}
					outputAmountFloat, err := strconv.ParseFloat(row[tx_OutputAmount], 64)
					if err != nil {
						fmt.Printf("TX %s: ExchangeToWithdraw Output Amount conversion error: %s, issue: %s\n", row[tx_ID], row[tx_OutputAmount], err)
					}
					if inputAmountFloat == -outputAmountFloat {
						valuesDiffer = false
					}
				}
				if valuesDiffer {
					fmt.Printf("TX %s: ExchangeToWithdraw currency amount error: input: %s, output: %s\n", row[tx_ID], row[tx_InputAmount], row[tx_OutputAmount])
				}
			}
			// [6] will be the dollar equivalent (just check that it starts '$)
			if row[tx_UsdEquivalent][0] != '$' {
				fmt.Printf("TX %s: ExchangeToWithdraw dollar equivalent invalid [%s]\n", row[tx_ID], row[tx_UsdEquivalent])
			}
			// [7] will be "approved / GBPX to GBP"
			if row[tx_Details] != "approved / GBPX to GBP" {
				fmt.Printf("TX %s: ExchangeToWithdraw details invalid [%s]\n", row[tx_ID], row[tx_Details])
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
			if (row[tx_InputCurrency] != "GBP") || (row[tx_OutputCurrency] != "GBP") {
				fmt.Printf("TX %s: ExchangeToWithdraw does not use GBP [%s,%s]\n", row[tx_ID], row[tx_InputCurrency], row[tx_OutputCurrency])
			}
			//if (row[tx_InputAmount] >= -22) || (row[tx_InputAmount] != -row[tx_OutputAmount]) {
			//	fmt.Printf("TX %s: ExchangeToWithdraw amount inconsistent [%s,%s]\n", row[tx_ID], row[tx_InputAmount], row[tx_OutputAmount])
			//}
			if row[tx_UsdEquivalent][0] != '$' {
				fmt.Printf("TX %s: ExchangeToWithdraw dollar equivalent invalid [%s]\n", row[tx_ID], row[tx_UsdEquivalent])
			}
			if row[tx_Details] != "approved / GBP withdrawal" {
				fmt.Printf("TX %s: ExchangeToWithdraw details invalid [%s]\n", row[tx_ID], row[tx_Details])
			}
			if len(exchangeToWithdraw) < 0 {
				fmt.Printf("TX %s: WithdrawExchanged with no matching ExchangeToWithdraw\n", row[tx_ID])
			} else {
				matchingExchangeToWithdraw := exchangeToWithdraw[0] // Get the presumed matching record
				exchangeToWithdraw = exchangeToWithdraw[1:]         // Remove that record from the FIFO
				// Both this record and the presumed matching ExchangeToWithdraw have been checked for validity.
				// To check for a match all that is needed is that "Input Amount" [3] "Output Currency" [4]
				// Note that "USD Equivalent" may not match presumably because the £/$ exchange rate may drift slightly
				// between the times when the ExchangeToWithdraw and the WithdrawExchanged happen.
				if (row[tx_InputAmount] != matchingExchangeToWithdraw[tx_InputAmount]) || (row[tx_OutputCurrency] != matchingExchangeToWithdraw[tx_OutputCurrency]) {
					fmt.Printf("TX %s: WithdrawExchanged finds non-matching ExchangeToWithdraw [TX: %s]\n", row[tx_ID], matchingExchangeToWithdraw[tx_ID])
				}
			}
			// Nothing needs to be recorded for a removal of fiat from NEXO
		case "DepositToExchange":
			// DepositToExchange represents the first of two operations that are involved in adding funds to NEXO.
			// There should be a correspodning (later) matching ExchangeDepositedOn that records the actual deposit of the funds.
			// For now it is assumed that the corresponding WithdrawExchanged records occur in the same order as the corresponding
			// ExchangeToWithdraw records so that all that is needed to match is a simple FIFO.
			// "Input Currency" will always be GBPX and "Output Currency" will always be GBP
			if (row[tx_InputCurrency] != "GBP") || (row[tx_OutputCurrency] != "GBPX") {
				fmt.Printf("TX %s: DepositToExchange does not use GBP [%s,%s]\n", row[tx_ID], row[tx_InputCurrency], row[tx_OutputCurrency])
			}
			// Input Amount and Output Amount must be identical.
			if row[tx_InputAmount] != row[tx_OutputAmount] {
				fmt.Printf("TX %s: DepositToExchange currency amount error: input: %s, output: %s\n", row[tx_ID], row[tx_InputAmount], row[tx_OutputAmount])
			}
			// "USD Equivalent" will be the dollar equivalent (just check that it starts '$)
			if row[tx_UsdEquivalent][0] != '$' {
				fmt.Printf("TX %s: DepositToExchange dollar equivalent invalid [%s]\n", row[tx_ID], row[tx_UsdEquivalent])
			}
			// "Details"" will be "approved / GBP Top Up"
			if row[tx_Details] != "approved / GBP Top Up" {
				fmt.Printf("TX %s: DepositToExchange details invalid [%s]\n", row[tx_ID], row[tx_Details])
			}
			depositToExchange = append(depositToExchange, row) // Add the record to the FIFO
		case "ExchangeDepositedOn":
			// ExchangeDepositedOn represents the second of two operations that are involved in depositing funds on NEXO.
			// This transaction records the actual deposit of GBP on NEXO.
			// There should be a corresponding (earlier) matching DepositToExchange.
			// [2] will always be GBPX
			// [3] will be a negative amount and [5] will be the corresponding positive amount
			// [4] will always be GBP
			// [6] will be the dollar equivalent (just check that it starts '$)
			// [7] will be "approved / GBPX to GBP"
			// [9] is date/time in CET
			if (row[tx_InputCurrency] != "GBP") || (row[tx_OutputCurrency] != "GBPX") {
				fmt.Printf("TX %s: ExchangeDepositedOn does not use GBP [%s,%s]\n", row[tx_ID], row[tx_InputCurrency], row[tx_OutputCurrency])
			}
			//if (row[tx_InputAmount] >= -22) || (row[tx_InputAmount] != -row[tx_OutputAmount]) {
			//	fmt.Printf("TX %s: ExchangeDepositedOn amount inconsistent [%s,%s]\n", row[tx_ID], row[tx_InputAmount], row[tx_OutputAmount])
			//}
			if row[tx_UsdEquivalent][0] != '$' {
				fmt.Printf("TX %s: ExchangeDepositedOn dollar equivalent invalid [%s]\n", row[tx_ID], row[tx_UsdEquivalent])
			}
			if row[tx_Details] != "approved / GBP to GBPX" {
				fmt.Printf("TX %s: ExchangeDepositedOn details invalid [%s]\n", row[tx_ID], row[tx_Details])
			}
			if len(exchangeToWithdraw) < 0 {
				fmt.Printf("TX %s: WithdrawExchanged with no matching ExchangeDepositedOn\n", row[tx_ID])
			} else {
				matchingDepositToExchange := depositToExchange[0] // Get the presumed matching record
				depositToExchange = depositToExchange[1:]         // Remove that record from the FIFO
				// Both this record and the presumed matching DepositToExchange have been checked for validity.
				// To check for a match all that is needed is that "Input Amount" and "Output Currency" match
				if (row[tx_InputAmount] != matchingDepositToExchange[tx_InputAmount]) || (row[tx_OutputCurrency] != matchingDepositToExchange[tx_OutputCurrency]) {
					fmt.Printf("TX %s: ExchangeDepositedOn finds non-matching ExchangeToWithdraw [TX: %s]\n", row[tx_ID], matchingDepositToExchange[tx_ID])
				}
				// Note that "USD Equivalent" may not match presumably because the £/$ exchange rate may drift slightly
				// between the times when the DepositToExchange and the ExchangeDepositedOn happen.
			}
			// Nothing needs to be recorded for a deposit of fiat into NEXO
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
