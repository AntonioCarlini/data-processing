package main

// This program takes a CSV from nexo.io and adjusts it into a format suitable for my records.
// Where applicable it rearranges each row into a format that matches my exchange-independent records.

// TODO
// The input and output filepaths are currently fixed. This should be changed
// The usage should be explained here
// Row processing should be enhanced to check all rows, even those that produce no output.

// Notes:
// Timestamps are in CET. These are NOT YET converted to UK local time.

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {

	flag.Parse()

	inputs := flag.Args()
	if len(inputs) != 2 {
		log.Fatalf("Exactly 2 arguments required but %d supplied\n", len(inputs))
	}

	transactionsFilename := flag.Arg(0)
	outputFile := flag.Arg(1)
	fmt.Printf("input transactions:     %s\n", transactionsFilename)
	fmt.Printf("output transactions:    %s\n", outputFile)

	transactions := readTransactions("/home/antonioc/Downloads/nexo-test.csv")

	convertedTransactions := convertTransactions(transactions)

	writeConvertedTransactions("/home/antonioc/Downloads/converted_nexo_transactions.csv", convertedTransactions)
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
	// The first element must match this exactly otherwise the format may have changed:
	expectedFirstRow := []string{"Transaction", "Type", "Currency", "Amount", "USD Equivalent", "Details", "Outstanding Loan", "Date / Time"}

	firstRow := transactions[0]

	if !testSlicesEqual(firstRow, expectedFirstRow) {
		log.Fatalf("First row fails to match")
	}

	output := make([][]string, 0)
	for i, row := range transactions[1:] {
		// "Interest" transactions need to be recorded as "STAKING"
		if row[1] == "Interest" {

			// [3] is amount of nexo
			// [4] is USD earned
			// [7] is date/time in CET
			// Output should be "nexo.io", date/time, uk date/time, nexo, (price), total, exch, £, "", "", "", "", "STAKING"
			if row[4][0] != '$' {
				log.Fatalf("Row %d is not in dollars [%s]", i, row[4])

			}
			entry := []string{"", "nexo.io", row[7], "", row[3], "", row[4][1:], "", "", "", "", "", "", "STAKING"}
			output = append(output, entry)
		}

		// "Deposit" transactions need to be recorded as "REWARD"
		if row[1] == "Deposit" {

			// [3] is amount of nexo
			// [4] is USD earned
			// [7] is date/time in CET
			// Output should be "nexo.io", date/time, uk date/time, nexo, (price), total, exch, £, "", "", "", "", "STAKING"
			if row[4][0] != '$' {
				log.Fatalf("Row %d is not in dollars [%s]", i, row[4])

			}
			entry := []string{"", "nexo.io", row[7], "", row[3], "", row[4][1:], "", "", "", "", "", "", "REWARD"}
			output = append(output, entry)
		}
	}

	// hacky code from stackoverflow to reverse the slice
	for i, j := 0, len(output)-1; i < j; i, j = i+1, j-1 {
		output[i], output[j] = output[j], output[i]
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

func testSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
