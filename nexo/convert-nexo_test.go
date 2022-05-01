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
const test_detail = "approved/detail text"
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
		testRow[tx_OutstandingLoan] = s.outstandingLoan
		output, exch2Withdraw, dep2Exchange, outputError := convertSingleTransaction(testRow)

		// output, exch2Withdraw and dep2Exchange should always be empty
		if output != "" {
			t.Errorf("output: got %q, wanted %q", output, "")
		}
		if exch2Withdraw != "" {
			t.Errorf("exch2Withdraw: got %q, wanted %q", exch2Withdraw, "")
		}
		if dep2Exchange != "" {
			t.Errorf("dep2Exchange: got %q, wanted %q", dep2Exchange, "")
		}

		if s.errorOutputExpected && (len(outputError) < 1) {
			t.Errorf("expected outputError missing:   for %s, got %q", s.outstandingLoan, outputError)
		}

		if !s.errorOutputExpected && (len(outputError) > 1) {
			t.Errorf("unexpected outputError present: for %s, got %q", s.outstandingLoan, outputError)
		}

	}
}

func buildStandardTestVector() []string {
	return []string{test_id, test_type, test_input_currency, test_input_amount, test_output_currency, test_output_amount, test_usd_equiv, test_detail, test_outstanding_loan, test_date}
}
