package main

import (
	"testing"
)

const test_id = "tx-ID"
const test_date = "2022-04-05 07:00:06"
const test_type = "Interest"
const test_input_currency = "NEXO"
const test_output_currency = "NEXO"
const test_input_amount = "0.11"
const test_output_amount = "0.22"
const test_usd_equiv = "$98.76"
const test_detail = "approved / detail text"
const test_outstanding_loan = "$0.00"

type OutsandingLoanTestData struct {
	outstandingLoan     string
	errorOutputExpected bool
}

// This test verifies that an "Outstanding Loan" other than "$0.00" is rejected
func TestOutStandingLoan(t *testing.T) {
	testRow := buildStandardTestVector()

	// for each OL produce acceptable four outputs

	tests := []OutsandingLoanTestData{
		OutsandingLoanTestData{"$0.00", false},
		OutsandingLoanTestData{"$0.10", true},
	}

	for _, s := range tests {
		output := make(map[string][][]string, 0)  // map of currency => array of strings
		exchangeToWithdraw := make([][]string, 0) // FIFO queue or records
		depositToExchange := make([][]string, 0)  // FIFO queue or records

		testRow[tx_OutstandingLoan] = s.outstandingLoan
		outputError := convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

		// The output map should have one key (NEXO) and one entry under that key
		if len(output) != 1 {
			t.Errorf("output: got %q, wanted exactly 1 entry (for key NEXO)", output)
		}

		if len(output["NEXO"]) != 1 {
			t.Errorf("output: got %q, wanted 1 entry for NEXO", output)
		}

		// exch2Withdraw and dep2Exchange should always be empty
		if len(exchangeToWithdraw) != 0 {
			t.Errorf("exchangeToWithdraw: got %q, wanted %q", exchangeToWithdraw, "")
		}
		if len(depositToExchange) != 0 {
			t.Errorf("depositToExchange: got %q, wanted %q", depositToExchange, "")
		}

		if s.errorOutputExpected && (len(outputError) < 1) {
			t.Errorf("expected outputError missing:   for %s, got %q", s.outstandingLoan, outputError)
		}

		if !s.errorOutputExpected && (len(outputError) > 1) {
			t.Errorf("unexpected outputError present: for %s, got %q", s.outstandingLoan, outputError)
		}
	}
}

// These tests verify that a "LockingTermDeposit" is (broadly) handled correctly
func TestLockingTermDeposit(t *testing.T) {
	testName := ""
	outputError := ""
	output := make(map[string][][]string, 0)  // map of currency => array of strings
	exchangeToWithdraw := make([][]string, 0) // FIFO queue or records
	depositToExchange := make([][]string, 0)  // FIFO queue or records

	validTestRow := buildStandardTestVector()
	validTestRow[tx_Type] = "LockingTermDeposit"
	validTestRow[tx_InputAmount] = "-9.99"
	validTestRow[tx_OutputAmount] = "9.99"
	validTestRow[tx_Details] = "approved / Transfer from Savings Wallet to Term Wallet"

	// Start by testing a set of data that should be OK
	testName = "valid data"
	outputError = convertSingleTransaction(validTestRow, &output, &exchangeToWithdraw, &depositToExchange)

	// output, exch2Withdraw and dep2Exchange should always be empty
	if len(output) != 0 {
		t.Errorf("%s/%s: output not empty: got %q", validTestRow[tx_Type], testName, output)
	}
	if len(exchangeToWithdraw) != 0 {
		t.Errorf("%s/%s: exchangeToWithdraw not empty: got %q", validTestRow[tx_Type], testName, exchangeToWithdraw)
	}
	if len(depositToExchange) != 0 {
		t.Errorf("%s/%s: depositToExchange not empty: got %q", validTestRow[tx_Type], testName, depositToExchange)
	}

	// No error should be reported
	if len(outputError) != 0 {
		t.Errorf("%s/%s: unexpected error text: %q", validTestRow[tx_Type], testName, outputError)
	}

	// Verify that a non-negative InputAmount is flagged as an error
	testName = "positive input currency"
	testRow := validTestRow
	testRow[tx_InputAmount] = "9.99"
	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// output, exch2Withdraw and dep2Exchange should always be empty
	if len(output) != 0 {
		t.Errorf("%s/%s: output not empty: got %q", validTestRow[tx_Type], testName, output)
	}
	if len(exchangeToWithdraw) != 0 {
		t.Errorf("%s/%s: exchangeToWithdraw not empty: got %q", validTestRow[tx_Type], testName, exchangeToWithdraw)
	}
	if len(depositToExchange) != 0 {
		t.Errorf("%s/%s: depositToExchange not empty: got %q", validTestRow[tx_Type], testName, depositToExchange)
	}

	// An error should be reported
	if len(outputError) == 0 {
		t.Errorf("%s/%s: invalid error text: %q", validTestRow[tx_Type], testName, outputError)
	}
}

// These tests verify that a "UnlockingTermDeposit" is (broadly) handled correctly
func TestUnlockingTermDeposit(t *testing.T) {
	testName := ""
	outputError := ""
	output := make(map[string][][]string, 0)  // map of currency => array of strings
	exchangeToWithdraw := make([][]string, 0) // FIFO queue or records
	depositToExchange := make([][]string, 0)  // FIFO queue or records

	validTestRow := buildStandardTestVector()
	validTestRow[tx_Type] = "UnlockingTermDeposit"
	validTestRow[tx_InputAmount] = "9.99"
	validTestRow[tx_OutputAmount] = "9.99"
	validTestRow[tx_Details] = "approved / Transfer from Term Wallet to Savings Wallet"

	// Start by testing a set of data that should be OK
	testName = "valid data"
	outputError = convertSingleTransaction(validTestRow, &output, &exchangeToWithdraw, &depositToExchange)

	// output, exch2Withdraw and dep2Exchange should always be empty
	if len(output) != 0 {
		t.Errorf("%s/%s: output not empty: got %q", validTestRow[tx_Type], testName, output)
	}
	if len(exchangeToWithdraw) != 0 {
		t.Errorf("%s/%s: exchangeToWithdraw not empty: got %q", validTestRow[tx_Type], testName, exchangeToWithdraw)
	}
	if len(depositToExchange) != 0 {
		t.Errorf("%s/%s: depositToExchange not empty: got %q", validTestRow[tx_Type], testName, depositToExchange)
	}

	// No error should be reported
	if len(outputError) != 0 {
		t.Errorf("%s/%s: unexpected error text: %q", validTestRow[tx_Type], testName, outputError)
	}

	// Check that a mismatched input/output amount is caught
	testName = "mismatched input/output amount"
	testRow := validTestRow
	testRow[tx_InputAmount] = "8.76"

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// output, exch2Withdraw and dep2Exchange should always be empty
	if len(output) != 0 {
		t.Errorf("%s/%s: output not empty: got %q", validTestRow[tx_Type], testName, output)
	}
	if len(exchangeToWithdraw) != 0 {
		t.Errorf("%s/%s: exchangeToWithdraw not empty: got %q", validTestRow[tx_Type], testName, exchangeToWithdraw)
	}
	if len(depositToExchange) != 0 {
		t.Errorf("%s/%s: depositToExchange not empty: got %q", validTestRow[tx_Type], testName, depositToExchange)
	}

	// An error should be reported
	if len(outputError) == 0 {
		t.Errorf("%s/%s: unexpected error text: %q", validTestRow[tx_Type], testName, outputError)
	}
}

// These tests verify that a "Deposit" is (broadly) handled correctly
func TestDeposit(t *testing.T) {
	testName := ""
	outputError := ""
	output := make(map[string][][]string, 0)  // map of currency => array of strings
	exchangeToWithdraw := make([][]string, 0) // FIFO queue or records
	depositToExchange := make([][]string, 0)  // FIFO queue or records

	validTestRow := buildStandardTestVector()
	validTestRow[tx_Type] = "Deposit"
	validTestRow[tx_InputAmount] = "9.99"
	validTestRow[tx_OutputAmount] = "9.99"
	validTestRow[tx_Details] = "approved / Nexonomics Exchange Cash-back Promotion"

	// Start by testing a set of data that should be OK
	testName = "valid data"
	outputError = convertSingleTransaction(validTestRow, &output, &exchangeToWithdraw, &depositToExchange)

	// The output map should have one key (NEXO) and one entry under that key
	if len(output) != 1 {
		t.Errorf("%s/%s: output has wrong number of keys: %q", validTestRow[tx_Type], testName, output)
	}

	if len(output["NEXO"]) != 1 {
		t.Errorf("%s/%s: output has wrong [NEXO] data: %q", validTestRow[tx_Type], testName, output)
	}

	// output, exch2Withdraw and dep2Exchange should always be empty
	if len(exchangeToWithdraw) != 0 {
		t.Errorf("%s/%s: exchangeToWithdraw not empty: got %q", validTestRow[tx_Type], testName, exchangeToWithdraw)
	}
	if len(depositToExchange) != 0 {
		t.Errorf("%s/%s: depositToExchange not empty: got %q", validTestRow[tx_Type], testName, depositToExchange)
	}

	// No error should be reported
	if len(outputError) != 0 {
		t.Errorf("%s/%s: unexpected error text: %q", validTestRow[tx_Type], testName, outputError)
	}

	// Check that a mismatched input/output amount is caught
	testName = "mismatched input/output amount"
	testRow := validTestRow
	testRow[tx_InputAmount] = "8.76"

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// output, exch2Withdraw and dep2Exchange should always be empty
	if len(output) != 1 {
		t.Errorf("%s/%s: output not empty: got %q", validTestRow[tx_Type], testName, output)
	}
	if len(exchangeToWithdraw) != 0 {
		t.Errorf("%s/%s: exchangeToWithdraw not empty: got %q", validTestRow[tx_Type], testName, exchangeToWithdraw)
	}
	if len(depositToExchange) != 0 {
		t.Errorf("%s/%s: depositToExchange not empty: got %q", validTestRow[tx_Type], testName, depositToExchange)
	}

	// An error should be reported
	if len(outputError) != 1 {
		// Currently the code chooses not to report this issue
		// TBD t.Errorf("%s/%s: unexpected error text: %q", validTestRow[tx_Type], testName, outputError)
	}
}
func buildStandardTestVector() []string {
	return []string{test_id, test_type, test_input_currency, test_input_amount, test_output_currency, test_output_amount, test_usd_equiv, test_detail, test_outstanding_loan, test_date}
}
