package main

import (
	"testing"
)

const test_id = "9876-5432-10"
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

// This test verifies that if a new transaction type appears, it will be flagged
func TestUnknownTransactionType(t *testing.T) {
	outputError := ""
	output := make(map[string][][]string, 0)  // map of currency => array of strings
	exchangeToWithdraw := make([][]string, 0) // FIFO queue or records
	depositToExchange := make([][]string, 0)  // FIFO queue or records

	testName := "inject unknown transaction"
	testRow := buildStandardTestVector()
	testRow[tx_Type] = "An Unexpected Transaction"

	// Start by testing a set of data that should be OK
	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// output, exch2Withdraw and dep2Exchange should always be empty
	if len(output) != 0 {
		t.Errorf("%s/%s: output not empty: got %q", testRow[tx_Type], testName, output)
	}
	if len(exchangeToWithdraw) != 0 {
		t.Errorf("%s/%s: exchangeToWithdraw not empty: got %q", testRow[tx_Type], testName, exchangeToWithdraw)
	}
	if len(depositToExchange) != 0 {
		t.Errorf("%s/%s: depositToExchange not empty: got %q", testRow[tx_Type], testName, depositToExchange)
	}

	// An error MUST be reported, so lack of error text is problematic
	if len(outputError) == 0 {
		t.Errorf("%s/%s: unexpected error text: %q", testRow[tx_Type], testName, outputError)
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
	testRow := make([]string, len(validTestRow))
	copy(testRow, validTestRow)

	testRow[tx_InputAmount] = "9.99"
	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// output, exch2Withdraw and dep2Exchange should always be empty
	if len(output) != 0 {
		t.Errorf("%s/%s: output not empty: got %q", testRow[tx_Type], testName, output)
	}
	if len(exchangeToWithdraw) != 0 {
		t.Errorf("%s/%s: exchangeToWithdraw not empty: got %q", testRow[tx_Type], testName, exchangeToWithdraw)
	}
	if len(depositToExchange) != 0 {
		t.Errorf("%s/%s: depositToExchange not empty: got %q", testRow[tx_Type], testName, depositToExchange)
	}

	// An error should be reported
	if len(outputError) == 0 {
		t.Errorf("%s/%s: invalid error text: %q", testRow[tx_Type], testName, outputError)
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
	testRow := make([]string, len(validTestRow))
	copy(testRow, validTestRow)
	testRow[tx_InputAmount] = "8.76"

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// output, exch2Withdraw and dep2Exchange should always be empty
	if len(output) != 0 {
		t.Errorf("%s/%s: output not empty: got %q", testRow[tx_Type], testName, output)
	}
	if len(exchangeToWithdraw) != 0 {
		t.Errorf("%s/%s: exchangeToWithdraw not empty: got %q", testRow[tx_Type], testName, exchangeToWithdraw)
	}
	if len(depositToExchange) != 0 {
		t.Errorf("%s/%s: depositToExchange not empty: got %q", testRow[tx_Type], testName, depositToExchange)
	}

	// An error should be reported
	if len(outputError) == 0 {
		t.Errorf("%s/%s: unexpected error text: %q", testRow[tx_Type], testName, outputError)
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
	testRow := make([]string, len(validTestRow))
	copy(testRow, validTestRow)
	testRow[tx_InputAmount] = "8.76"

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// output, exch2Withdraw and dep2Exchange should always be empty
	if len(output) != 1 {
		t.Errorf("%s/%s: output not empty: got %q", testRow[tx_Type], testName, output)
	}
	if len(exchangeToWithdraw) != 0 {
		t.Errorf("%s/%s: exchangeToWithdraw not empty: got %q", testRow[tx_Type], testName, exchangeToWithdraw)
	}
	if len(depositToExchange) != 0 {
		t.Errorf("%s/%s: depositToExchange not empty: got %q", testRow[tx_Type], testName, depositToExchange)
	}

	// An error should be reported
	if len(outputError) != 1 {
		// Currently the code chooses not to report this issue
		// TBD t.Errorf("%s/%s: unexpected error text: %q", validTestRow[tx_Type], testName, outputError)
	}
}

// These tests verify that a "ExchangeToWithdraw" is (broadly) handled correctly
func TestExchangeToWithdraw(t *testing.T) {
	testName := ""
	outputError := "outputError"
	output := make(map[string][][]string, 0)  // map of currency => array of strings
	exchangeToWithdraw := make([][]string, 0) // FIFO queue or records
	depositToExchange := make([][]string, 0)  // FIFO queue or records

	validTestRow := buildStandardTestVector()
	validTestRow[tx_Type] = "ExchangeToWithdraw"
	validTestRow[tx_InputCurrency] = "GBPX"
	validTestRow[tx_OutputCurrency] = "GBP"
	validTestRow[tx_InputAmount] = "-9.99"
	validTestRow[tx_OutputAmount] = "9.99"
	validTestRow[tx_Details] = "approved / GBPX to GBP"

	// Start by testing a set of data that should be OK
	testName = "valid data"
	outputError = convertSingleTransaction(validTestRow, &output, &exchangeToWithdraw, &depositToExchange)

	// output should always be empty
	if len(output) != 0 {
		t.Errorf("%s/%s: output not empty: got %q", validTestRow[tx_Type], testName, output)
	}
	// exchangeToWithdraw should exactly match validTestRow
	if len(exchangeToWithdraw) == 0 {
		t.Errorf("%s/%s: exchangeToWithdraw unexpectedly emptyempty", validTestRow[tx_Type], testName)
	} else if len(exchangeToWithdraw) != 1 {
		t.Errorf("%s/%s: exchangeToWithdraw has too many entries: got %q, expected %q", validTestRow[tx_Type], testName, exchangeToWithdraw, validTestRow)
	} else {
		if !testSlicesEqual(exchangeToWithdraw[0], validTestRow) {
			t.Errorf("%s/%s: exchangeToWithdraw has bad contents: got %q, expected %q", validTestRow[tx_Type], testName, exchangeToWithdraw, validTestRow)
		}
	}
	// depositToExchange should always be empty
	if len(depositToExchange) != 0 {
		t.Errorf("%s/%s: depositToExchange not empty: got %q", validTestRow[tx_Type], testName, depositToExchange)
	}

	// No error should be reported
	if len(outputError) != 0 {
		t.Errorf("%s/%s: unexpected error text: %q", validTestRow[tx_Type], testName, outputError)
	}

	// Check that a mismatched input/output amount is caught
	testName = "mismatched input/output amount"
	testRow := make([]string, len(validTestRow))
	copy(testRow, validTestRow)
	testRow[tx_InputAmount] = "8.76"

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// An error should be reported
	if len(outputError) == 0 {
		t.Errorf("%s/%s: unexpected error text: %q", testRow[tx_Type], testName, outputError)
	}

	// Check that an Input Currency other than GBPX is caught
	testName = "mismatched input/output amount"
	copy(testRow, validTestRow)
	testRow[tx_InputCurrency] = "BAD-CURRENCY"

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// An error should be reported
	if len(outputError) == 0 {
		t.Errorf("%s/%s: unexpected error text: %q", testRow[tx_Type], testName, outputError)
	}

	// Check that an Output Currency other than GBP is caught
	testName = "mismatched input/output amount"
	copy(testRow, validTestRow)
	testRow[tx_OutputCurrency] = "BAD-CURRENCY"

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// An error should be reported
	if len(outputError) == 0 {
		t.Errorf("%s/%s: unexpected error text: %q", testRow[tx_Type], testName, outputError)
	}
}

// These tests verify that a "WithdrawExchanged" is (broadly) handled correctly
func TestWithdrawExchanged(t *testing.T) {
	testName := ""
	outputError := ""
	output := make(map[string][][]string, 0) // map of currency => array of strings
	depositToExchange := make([][]string, 0) // FIFO queue or records

	exchangeToWithdrawRow := buildStandardTestVector()
	exchangeToWithdrawRow[tx_Type] = "ExchangeToWithdraw"
	exchangeToWithdrawRow[tx_InputAmount] = "9.99"
	exchangeToWithdrawRow[tx_InputCurrency] = "GBP"
	exchangeToWithdrawRow[tx_OutputCurrency] = "GBP"
	exchangeToWithdraw := [][]string{exchangeToWithdrawRow}

	validTestRow := buildStandardTestVector()
	validTestRow[tx_Type] = "WithdrawExchanged"
	validTestRow[tx_InputAmount] = "9.99"
	validTestRow[tx_InputCurrency] = "GBP"
	validTestRow[tx_OutputAmount] = "9.99"
	validTestRow[tx_OutputCurrency] = "GBP"
	validTestRow[tx_Details] = "approved / GBP withdrawal"

	// Start by testing a set of data that should be OK
	testName = "valid data"
	outputError = convertSingleTransaction(validTestRow, &output, &exchangeToWithdraw, &depositToExchange)

	// output, exch2Withdraw and dep2Exchange should always be empty
	if len(output) != 0 {
		t.Errorf("%s/%s: output not empty: got %q", validTestRow[tx_Type], testName, output)
	}
	if len(exchangeToWithdraw) != 0 {
		t.Errorf("%s/%s: exchangeToWithdraw should be empty: got %q", validTestRow[tx_Type], testName, exchangeToWithdraw)
	}
	if len(depositToExchange) != 0 {
		t.Errorf("%s/%s: depositToExchange not empty: got %q", validTestRow[tx_Type], testName, depositToExchange)
	}

	// No error should be reported
	if len(outputError) != 0 {
		t.Errorf("%s/%s: unexpected error text: %q", validTestRow[tx_Type], testName, outputError)
	}

	// Check that a mismatched input amount with ExchangeToWithdraw is caught
	testName = "mismatched input amount"
	testRow := make([]string, len(validTestRow))
	copy(testRow, validTestRow)
	testRow[tx_InputAmount] = "8.76"
	exchangeToWithdraw = [][]string{exchangeToWithdrawRow}

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// An error should be reported
	if len(outputError) == 0 {
		t.Errorf("%s/%s: unexpected error text: %q", testRow[tx_Type], testName, outputError)
	}

	// Check that a mismatched output currency with ExchangeToWithdraw is caught
	testName = "mismatched input amount"
	testRow = make([]string, len(validTestRow))
	copy(testRow, validTestRow)
	testRow[tx_OutputCurrency] = "NEXO"
	exchangeToWithdraw = [][]string{exchangeToWithdrawRow}

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// An error should be reported
	if len(outputError) == 0 {
		t.Errorf("%s/%s: unexpected error text: %q", testRow[tx_Type], testName, outputError)
	}
}

// These tests verify that a "DepositToExchange" is (broadly) handled correctly
func TestDepositToExchange(t *testing.T) {
	testName := ""
	outputError := "outputError"
	output := make(map[string][][]string, 0)  // map of currency => array of strings
	exchangeToWithdraw := make([][]string, 0) // FIFO queue or records
	depositToExchange := make([][]string, 0)  // FIFO queue or records

	validTestRow := buildStandardTestVector()
	validTestRow[tx_Type] = "DepositToExchange"
	validTestRow[tx_InputCurrency] = "GBP"
	validTestRow[tx_OutputCurrency] = "GBPX"
	validTestRow[tx_InputAmount] = "9.99"
	validTestRow[tx_OutputAmount] = "9.99"
	validTestRow[tx_Details] = "approved / GBP Top Up"

	// Start by testing a set of data that should be OK
	testName = "valid data"
	outputError = convertSingleTransaction(validTestRow, &output, &exchangeToWithdraw, &depositToExchange)

	// output should always be empty
	if len(output) != 0 {
		t.Errorf("%s/%s: output not empty: got %q", validTestRow[tx_Type], testName, output)
	}
	// exchangeToWithdraw should always be empty
	if len(exchangeToWithdraw) != 0 {
		t.Errorf("%s/%s: exchangeToWithdraw not empty: got %q", validTestRow[tx_Type], testName, exchangeToWithdraw)
	}
	// exchangeToWithdraw should exactly match validTestRow
	if len(depositToExchange) == 0 {
		t.Errorf("%s/%s: depositToExchange unexpectedly emptyempty", validTestRow[tx_Type], testName)
	} else if len(depositToExchange) != 1 {
		t.Errorf("%s/%s: depositToExchange has too many entries: got %q, expected %q", validTestRow[tx_Type], testName, depositToExchange, validTestRow)
	} else {
		if !testSlicesEqual(depositToExchange[0], validTestRow) {
			t.Errorf("%s/%s: depositToExchange has bad contents: got %q, expected %q", validTestRow[tx_Type], testName, depositToExchange, validTestRow)
		}
	}

	// No error should be reported
	if len(outputError) != 0 {
		t.Errorf("%s/%s: unexpected error text: %q", validTestRow[tx_Type], testName, outputError)
	}

	// Check that a mismatched input/output amount is caught
	testName = "mismatched input/output amount"
	testRow := make([]string, len(validTestRow))
	copy(testRow, validTestRow)
	testRow[tx_InputAmount] = "8.76"

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// An error should be reported
	if len(outputError) == 0 {
		t.Errorf("%s/%s: unexpected error text: %q", testRow[tx_Type], testName, outputError)
	}

	// Check that an Input Currency other than GBP is caught
	testName = "mismatched input/output amount"
	copy(testRow, validTestRow)
	testRow[tx_InputCurrency] = "BAD-CURRENCY"

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// An error should be reported
	if len(outputError) == 0 {
		t.Errorf("%s/%s: unexpected error text: %q", testRow[tx_Type], testName, outputError)
	}

	// Check that an Output Currency other than GBPX is caught
	testName = "mismatched input/output amount"
	copy(testRow, validTestRow)
	testRow[tx_OutputCurrency] = "BAD-CURRENCY"

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// An error should be reported
	if len(outputError) == 0 {
		t.Errorf("%s/%s: unexpected error text: %q", testRow[tx_Type], testName, outputError)
	}
}

// These tests verify that a "ExchangeDepositedOn" is (broadly) handled correctly
func TestExchangeDepositedOn(t *testing.T) {
	testName := ""
	outputError := ""
	output := make(map[string][][]string, 0)  // map of currency => array of strings
	exchangeToWithdraw := make([][]string, 0) // FIFO queue or records
	depositToExchange := make([][]string, 0)  // FIFO queue or records

	depositToExchangeRow := buildStandardTestVector()
	depositToExchangeRow[tx_ID] = "TX-ID-DEP2EXCH"
	depositToExchangeRow[tx_Type] = "DepositToExchange"
	depositToExchangeRow[tx_InputAmount] = "9.99"
	depositToExchangeRow[tx_InputCurrency] = "GBP"
	depositToExchangeRow[tx_OutputAmount] = "9.99"
	depositToExchangeRow[tx_OutputCurrency] = "GBPX"
	depositToExchange = [][]string{depositToExchangeRow}

	validTestRow := buildStandardTestVector()
	validTestRow[tx_Type] = "ExchangeDepositedOn"
	validTestRow[tx_InputAmount] = "9.99"
	validTestRow[tx_InputCurrency] = "GBP"
	validTestRow[tx_OutputAmount] = "9.99"
	validTestRow[tx_OutputCurrency] = "GBPX"
	validTestRow[tx_Details] = "approved / GBP to GBPX"

	// Start by testing a set of data that should be OK
	testName = "valid data"
	outputError = convertSingleTransaction(validTestRow, &output, &exchangeToWithdraw, &depositToExchange)

	// output, exch2Withdraw and dep2Exchange should always be empty
	if len(output) != 0 {
		t.Errorf("%s/%s: output not empty: got %q", validTestRow[tx_Type], testName, output)
	}
	if len(exchangeToWithdraw) != 0 {
		t.Errorf("%s/%s: exchangeToWithdraw should be empty: got %q", validTestRow[tx_Type], testName, exchangeToWithdraw)
	}
	if len(depositToExchange) != 0 {
		t.Errorf("%s/%s: depositToExchange not empty: got %q", validTestRow[tx_Type], testName, depositToExchange)
	}

	// No error should be reported
	if len(outputError) != 0 {
		t.Errorf("%s/%s: unexpected error text: %q", validTestRow[tx_Type], testName, outputError)
	}

	// Check that a mismatched input amount with DepositToExchange is caught
	testName = "mismatched input amount"
	testRow := make([]string, len(validTestRow))
	copy(testRow, validTestRow)
	testRow[tx_InputAmount] = "8.76"
	depositToExchange = [][]string{depositToExchangeRow}

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// An error should be reported
	if len(outputError) == 0 {
		t.Errorf("%s/%s: unexpected error text: %q", testRow[tx_Type], testName, outputError)
	}

	// Check that a mismatched output currency with ExchangeToWithdraw is caught
	testName = "mismatched input amount"
	testRow = make([]string, len(validTestRow))
	copy(testRow, validTestRow)
	testRow[tx_OutputCurrency] = "NEXO"
	depositToExchange = [][]string{depositToExchangeRow}

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// An error should be reported
	if len(outputError) == 0 {
		t.Errorf("%s/%s: unexpected error text: %q", testRow[tx_Type], testName, outputError)
	}
}

// These tests verify that a "Interest" is (broadly) handled correctly
func TestInterest(t *testing.T) {
	InterestTransaction(t, "Interest")
}

// These tests verify that a "FixedTermInterest" is (broadly) handled correctly
func TestFixedTermInterest(t *testing.T) {
	InterestTransaction(t, "FixedTermInterest")
}

// This is a support function that handles "Interest" and "FixedInterest".
// The handling for each type is currently identical.
func InterestTransaction(t *testing.T, pType string) {
	testName := ""
	outputError := ""
	output := make(map[string][][]string, 0)  // map of currency => array of strings
	exchangeToWithdraw := make([][]string, 0) // FIFO queue or records
	depositToExchange := make([][]string, 0)  // FIFO queue or records

	validTestRow := buildStandardTestVector()
	validTestRow[tx_Type] = pType
	validTestRow[tx_Details] = "approved / this is not tested"

	// Start by testing a set of data that should be OK
	testName = "valid data"
	outputError = convertSingleTransaction(validTestRow, &output, &exchangeToWithdraw, &depositToExchange)

	// The output map should have one key (NEXO) and one entry under that key
	if len(output) != 1 {
		t.Errorf("%s/%s: output has wrong number of keys: %q", validTestRow[tx_Type], testName, output)
	} else if len(output["NEXO"]) != 1 {
		t.Errorf("%s/%s: output has wrong [NEXO] data: %q", validTestRow[tx_Type], testName, output)
	} else if output["NEXO"][0][13] != "STAKING" {
		t.Errorf("%s/%s: output has wrong event (expected STAKING): %q", validTestRow[tx_Type], testName, output)
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
	testName = "invalid details"
	testRow := make([]string, len(validTestRow))
	copy(testRow, validTestRow)
	testRow[tx_Details] = "unapproved"

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// An error should be reported
	if len(outputError) == 0 {
		t.Errorf("%s/%s: unexpected error text: %q", testRow[tx_Type], testName, outputError)
	}
}

// These tests verify that a "Exchange" is (broadly) handled correctly
// The code here specifically uses the format that was current until sometime in April 2022.
func TestExchangePreMay2022(t *testing.T) {
	testName := ""
	outputError := ""
	output := make(map[string][][]string, 0)  // map of currency => array of strings
	exchangeToWithdraw := make([][]string, 0) // FIFO queue or records
	depositToExchange := make([][]string, 0)  // FIFO queue or records

	validTestRow := buildStandardTestVector()
	validTestRow[tx_Type] = "Exchange"
	validTestRow[tx_InputCurrency] = "GBPX/UST"
	validTestRow[tx_InputAmount] = "UST 9.99"
	validTestRow[tx_OutputCurrency] = "UST"
	validTestRow[tx_OutputAmount] = "9.99"
	validTestRow[tx_Details] = "approved / Exchange GBPX to TerraUSD"

	// Start by testing a set of data that should be OK
	testName = "valid GBP purchase"
	outputError = convertSingleTransaction(validTestRow, &output, &exchangeToWithdraw, &depositToExchange)

	// The output map should have one key (NEXO) and one entry under that key
	if len(output) != 1 {
		t.Errorf("%s/%s: output has wrong number of keys: %q", validTestRow[tx_Type], testName, output)
	}

	if len(output["UST"]) != 1 {
		t.Errorf("%s/%s: output has missing [UST] data: %q", validTestRow[tx_Type], testName, output)
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
	testName = "valid swap"
	testRow := make([]string, len(validTestRow))
	copy(testRow, validTestRow)
	testRow[tx_InputCurrency] = "NEXO/UST"
	output = make(map[string][][]string, 0)

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// output, exch2Withdraw and dep2Exchange should always be empty
	if len(output) != 2 {
		t.Errorf("%s/%s: output not valid: got %q", testRow[tx_Type], testName, output)
	}

	// There shoul be a SELL event for NEXO
	if len(output["NEXO"]) != 1 {
		t.Errorf("%s/%s: output has missing [NEXO] data: %q", testRow[tx_Type], testName, output)
	} else if output["NEXO"][0][13] != "SELL" {
		t.Errorf("%s/%s: output has wrong NEXO event (expected SELL): %q", testRow[tx_Type], testName, output)
	}

	// There shoul be a BUY event for UST
	if len(output["UST"]) != 1 {
		t.Errorf("%s/%s: output has missing [UST] data: %q", testRow[tx_Type], testName, output)
	} else if output["UST"][0][13] != "BUY" {
		t.Errorf("%s/%s: output has wrong UST event (expected BUY): %q", testRow[tx_Type], testName, output)
	}

	if len(exchangeToWithdraw) != 0 {
		t.Errorf("%s/%s: exchangeToWithdraw not empty: got %q", testRow[tx_Type], testName, exchangeToWithdraw)
	}
	if len(depositToExchange) != 0 {
		t.Errorf("%s/%s: depositToExchange not empty: got %q", testRow[tx_Type], testName, depositToExchange)
	}

	// No error should be reported
	if len(outputError) != 0 {
		t.Errorf("%s/%s: unexpected error text: %q", testRow[tx_Type], testName, outputError)
	}
}

// These tests verify that a "Withdrawal" is (broadly) handled correctly
func TestWithdrawal(t *testing.T) {

	testName := ""
	outputError := ""
	output := make(map[string][][]string, 0)  // map of currency => array of strings
	exchangeToWithdraw := make([][]string, 0) // FIFO queue or records
	depositToExchange := make([][]string, 0)  // FIFO queue or records

	validTestRow := buildStandardTestVector()
	validTestRow[tx_Type] = "Withdrawal"
	validTestRow[tx_InputAmount] = "9.99"
	validTestRow[tx_OutputAmount] = "9.99"
	validTestRow[tx_InputCurrency] = "BTC"
	validTestRow[tx_OutputCurrency] = "BTC"
	validTestRow[tx_Details] = "approved / any text"

	// Start by testing a set of data that should be OK
	testName = "valid data"
	outputError = convertSingleTransaction(validTestRow, &output, &exchangeToWithdraw, &depositToExchange)

	// The output map should have one key (BTC) and one entry under that key
	if len(output) != 1 {
		t.Errorf("%s/%s: output has wrong number of keys: %q", validTestRow[tx_Type], testName, output)
	}

	if len(output["BTC"]) != 1 {
		t.Errorf("%s/%s: output has wrong [BTC] data: %q", validTestRow[tx_Type], testName, output)
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
	testRow := make([]string, len(validTestRow))
	copy(testRow, validTestRow)
	testRow[tx_InputAmount] = "8.76"

	outputError = convertSingleTransaction(testRow, &output, &exchangeToWithdraw, &depositToExchange)

	// output, exch2Withdraw and dep2Exchange should always be empty
	if len(output) != 1 {
		t.Errorf("%s/%s: output not empty: got %q", testRow[tx_Type], testName, output)
	}
	if len(exchangeToWithdraw) != 0 {
		t.Errorf("%s/%s: exchangeToWithdraw not empty: got %q", testRow[tx_Type], testName, exchangeToWithdraw)
	}
	if len(depositToExchange) != 0 {
		t.Errorf("%s/%s: depositToExchange not empty: got %q", testRow[tx_Type], testName, depositToExchange)
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
