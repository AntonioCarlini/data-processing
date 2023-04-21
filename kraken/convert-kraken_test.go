package main

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/superoo7/go-gecko/v3/types"
)

type CGNameLookupTest struct {
	symbol string
	result string
	found  bool
}

// Checks ConvertCoinSymbolToCoingeckoCoinName
func TestCoinNameConversion(t *testing.T) {
	var Tests = []CGNameLookupTest{
		{"BTC", "bitcoin", true},
		{"ImadeThisUp", "", false},
		{"ETH", "ethereum", true},
		{"FLOW", "flow", true},
		{"ADA", "cardano", true},
	}

	for _, test := range Tests {
		result, found := ConvertCoinSymbolToCoingeckoCoinName(test.symbol)
		if (result != test.result) || (found != test.found) {
			t.Errorf("Converted %q, got %q/%t but wanted %q/%t", test.symbol, result, found, test.result, test.found)
		}
	}
}

type DatePriceInfo struct {
	dateTime string
	price    float32
}

type TestAndResults struct {
	name      string
	testcases []DatePriceInfo
	results   map[string]float32
	success   bool
}

// Checks ConvertTimePriceHistoryToDailyPriceHistory
func TestConvertToPriceHistory(t *testing.T) {
	// Case 1: Normal case, a few days in a row, some needing an average, all one day apart
	var Test1 = []DatePriceInfo{
		{"2021-10-21 09:00:01", 32000.00},
		{"2021-10-21 12:00:01", 31000.00},
		{"2021-10-21 18:00:01", 33000.00},
		{"2021-10-22 18:00:01", 35000.00},
		{"2021-10-23 09:00:01", 35000.00},
		{"2021-10-23 12:00:01", 37000.00},
		{"2021-10-23 18:00:01", 39000.00},
		{"2021-10-23 20:00:01", 41000.00},
	}
	var ExpectedTestResult1 = map[string]float32{
		"2021-10-21": 32000.00,
		"2021-10-22": 35000.00,
		"2021-10-23": 38000.00,
	}

	// Case 2: Normal case, a few days in a row, some 1 day apart, some 2 days apart
	var Test2 = []DatePriceInfo{
		{"2021-10-21 09:00:01", 32000.00},
		{"2021-10-22 18:00:01", 35000.00},
		{"2021-10-23 20:00:01", 41000.00},
		{"2021-10-25 07:00:01", 31000.00},
		{"2021-10-27 18:00:01", 51000.00},
		{"2021-10-28 11:00:01", 49000.00},
	}
	var ExpectedTestResult2 = map[string]float32{
		"2021-10-21": 32000.00,
		"2021-10-22": 35000.00,
		"2021-10-23": 41000.00,
		"2021-10-25": 31000.00,
		"2021-10-27": 51000.00,
		"2021-10-28": 49000.00,
	}

	// Case 3: Failure case, a few days in a row, some 1 day apart, some 2 days apart, one gap of more than 2 days
	var Test3 = []DatePriceInfo{
		{"2021-10-21 09:00:01", 32000.00},
		{"2021-10-22 18:00:01", 35000.00},
		{"2021-10-23 20:00:01", 41000.00},
		{"2021-10-26 07:00:01", 31000.00}, // 3-day gap
		{"2021-10-27 18:00:01", 51000.00},
		{"2021-10-28 11:00:01", 49000.00},
	}
	var ExpectedTestResult3 = map[string]float32{
		"2021-10-21": 32000.00,
		"2021-10-22": 35000.00,
		"2021-10-23": 41000.00,
		"2021-10-25": 31000.00,
		"2021-10-27": 51000.00,
		"2021-10-28": 49000.00,
	}

	// Case 4: Normal case, a few days in a row, crosses a year boundary, all one day apart
	var Test4 = []DatePriceInfo{
		{"2021-12-21 09:00:01", 32000.00},
		{"2021-12-22 18:00:01", 35000.00},
		{"2021-12-23 20:00:01", 41000.00},
		{"2021-12-24 07:00:01", 31000.00},
		{"2021-12-25 18:00:01", 51000.00},
		{"2021-12-26 11:00:01", 49000.00},
		{"2021-12-27 14:00:01", 49000.00},
		{"2021-12-28 15:00:01", 50000.00},
		{"2021-12-29 13:00:01", 51000.00},
		{"2021-12-30 12:00:01", 48000.00},
		{"2021-12-31 07:00:01", 37000.00}, // End of year
		{"2022-01-01 11:00:01", 35000.00}, // Start of new year
		{"2022-01-02 01:00:01", 100000.00},
		{"2022-01-03 23:00:01", 7000.00},
	}
	var ExpectedTestResult4 = map[string]float32{
		"2021-12-21": 32000.00,
		"2021-12-22": 35000.00,
		"2021-12-23": 41000.00,
		"2021-12-24": 31000.00,
		"2021-12-25": 51000.00,
		"2021-12-26": 49000.00,
		"2021-12-27": 49000.00,
		"2021-12-28": 50000.00,
		"2021-12-29": 51000.00,
		"2021-12-30": 48000.00,
		"2021-12-31": 37000.00, // End of year
		"2022-01-01": 35000.00, // Start of new year
		"2022-01-02": 100000.00,
		"2022-01-03": 7000.00,
	}

	// Case 5: Normal case, a few days in a row, crosses a year boundary, all one day apart except year boundary which is DEC-30, JAN-01
	var Test5 = []DatePriceInfo{
		{"2021-12-27 14:00:01", 49000.00},
		{"2021-12-28 15:00:01", 50000.00},
		{"2021-12-29 13:00:01", 51000.00},
		{"2021-12-30 12:00:01", 48000.00},
		{"2022-01-01 11:00:01", 35000.00}, // Start of new year
		{"2022-01-02 01:00:01", 100000.00},
		{"2022-01-03 23:00:01", 7000.00},
	}
	var ExpectedTestResult5 = map[string]float32{
		"2021-12-27": 49000.00,
		"2021-12-28": 50000.00,
		"2021-12-29": 51000.00,
		"2021-12-30": 48000.00,
		"2022-01-01": 35000.00, // Start of new year
		"2022-01-02": 100000.00,
		"2022-01-03": 7000.00,
	}

	// Case 6: Normal case, a few days in a row, crosses a year boundary, all one day apart except year boundary which is DEC-31, JAN-02
	var Test6 = []DatePriceInfo{
		{"2021-12-27 14:00:01", 49000.00},
		{"2021-12-28 15:00:01", 50000.00},
		{"2021-12-29 13:00:01", 51000.00},
		{"2021-12-30 12:00:01", 48000.00},
		{"2021-12-31 07:00:01", 37000.00},
		{"2022-01-02 11:00:02", 35000.00},
		{"2022-01-03 23:00:01", 7000.00},
	}
	var ExpectedTestResult6 = map[string]float32{
		"2021-12-27": 49000.00,
		"2021-12-28": 50000.00,
		"2021-12-29": 51000.00,
		"2021-12-30": 48000.00,
		"2021-12-31": 37000.00,
		"2022-01-02": 35000.00, // Start of new year
		"2022-01-03": 7000.00,
	}

	// Case 7: Failure case, a few days in a row, crosses a year boundary, all one day apart except year boundary which is DEC-30, JAN-02
	var Test7 = []DatePriceInfo{
		{"2021-12-27 14:00:01", 49000.00},
		{"2021-12-28 15:00:01", 50000.00},
		{"2021-12-29 13:00:01", 51000.00},
		{"2021-12-30 12:00:01", 48000.00},
		{"2022-01-02 11:00:02", 35000.00}, // Start of new year
		{"2022-01-03 23:00:01", 7000.00},
	}
	var ExpectedTestResult7 = map[string]float32{
		"2021-12-27": 49000.00,
		"2021-12-28": 50000.00,
		"2021-12-29": 51000.00,
		"2021-12-30": 48000.00,
		"2022-01-02": 35000.00, // Start of new year
		"2022-01-03": 7000.00,
	}

	// Case 8: Failure case, a few days in a row, all one day apart but one entry steps back one day
	var Test8 = []DatePriceInfo{
		{"2021-12-21 09:00:01", 32000.00},
		{"2021-12-22 18:00:01", 35000.00},
		{"2021-12-23 20:00:01", 41000.00},
		{"2021-12-24 07:00:01", 31000.00},
		{"2021-12-25 18:00:01", 51000.00},
		{"2021-12-26 11:00:01", 49000.00},
		{"2021-12-27 14:00:01", 49000.00},
		{"2021-12-26 15:00:01", 50000.00}, // Goes backwards
		{"2021-12-29 13:00:01", 51000.00},
		{"2021-12-30 12:00:01", 48000.00},
		{"2021-12-31 07:00:01", 37000.00},
		{"2022-01-01 11:00:01", 35000.00},
		{"2022-01-02 01:00:01", 100000.00},
		{"2022-01-03 23:00:01", 7000.00},
	}
	var ExpectedTestResult8 = map[string]float32{
		"2021-12-21": 32000.00,
		"2021-12-22": 35000.00,
		"2021-12-23": 41000.00,
		"2021-12-24": 31000.00,
		"2021-12-25": 51000.00,
		"2021-12-26": 49000.00,
		"2021-12-27": 49000.00,
		"2021-12-28": 50000.00,
		"2021-12-29": 51000.00,
		"2021-12-30": 48000.00,
		"2021-12-31": 37000.00, // End of year
		"2022-01-01": 35000.00, // Start of new year
		"2022-01-02": 100000.00,
		"2022-01-03": 7000.00,
	}
	var allTests = []TestAndResults{
		{"Test 1", Test1, ExpectedTestResult1, true},
		{"Test 2", Test2, ExpectedTestResult2, true},
		{"Test 3", Test3, ExpectedTestResult3, false},
		{"Test 4", Test4, ExpectedTestResult4, true},
		{"Test 5", Test5, ExpectedTestResult5, true},
		{"Test 6", Test6, ExpectedTestResult6, true},
		{"Test 7", Test7, ExpectedTestResult7, false},
		{"Test 8", Test8, ExpectedTestResult8, false},
	}

	var verbose bool = false // Set to true to make ConvertTimePriceHistoryToDailyPriceHistory display internal state
	var TestData []types.ChartItem

	for _, test := range allTests {
		TestData = nil
		if verbose {
			fmt.Printf("Starting %q\n", test.name)
		}
		for _, v := range test.testcases {
			tm, err := time.Parse("2006-01-02 15:04:05", v.dateTime)
			if err != nil {
				t.Errorf("Error in %q preparing date %q: %q", test.name, v.dateTime, err)
			}
			TestData = append(TestData, types.ChartItem([2]float32{float32(tm.Unix() * 1000), v.price}))
		}
		result, err := ConvertTimePriceHistoryToDailyPriceHistory(TestData, verbose)
		if (test.success) && (err != nil) {
			t.Errorf("Error in %q should succeed but error reported: %q", test.name, err)
		} else if (!test.success) && (err == nil) {
			t.Errorf("Error in %q should fail but no error reported", test.name)
		} else if err == nil {
			// Here there is (correctly) no error reported so the resulting map
			// must exactly match expectations
			if verbose {
				for k, v := range result {
					fmt.Printf("%q  -> %0.04f\n", k, v)
				}
			}
			eq := reflect.DeepEqual(result, test.results)
			if !eq {
				t.Errorf("Error in %q result not as expected", test.name)
			}
		}
	}
}
