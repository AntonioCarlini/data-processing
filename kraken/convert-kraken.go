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
// These notes apply in late 2024:
//
// Staking reward:
//    - "staking" of token.S with non-blank balance
//
// Purchase of a token:
//    - "spend" of token with refid REF-A
//    - "receive" of token with refid REF-A
//
// The notes below are historical ones from before 2024:
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
// Staking of a token
//    - "withdrawal" of token with blank txid and refid REF-A
//    - "deposit" of token.S with blank txid and refid REF-B
//    - "transfer" of token.S with refid REF-B and and subtype "stakingfromspot"
//    - "transfer" of token with refid REF-A and subtype "spottostaking"
//
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

type ledger struct {
	row     int
	txid    string
	refid   string
	time    string
	format  string
	subtype string
	aclass  string
	asset   string
	wallet  string
	amount  string
	fee     string
	balance string
}

var historicalPriceCache = map[string]string{}
var historicalPriceCacheUpdated bool = false

// Open the input file, convert it to the output format and write it out in CSV format
func main() {

	cliHpdPath := flag.String("cache", "", "Directory that contains the historical price data (CSV)")
	flag.Parse()

	home := os.Getenv("HOME")
	hpdFilename := home + "/.config/coin-prices/price-data/"
	if *cliHpdPath != "" {
		hpdFilename = *cliHpdPath
	}

	inputs := flag.Args()
	if len(inputs) != 2 {
		log.Fatalf("Exactly 2 arguments required but %d supplied\n", len(inputs))
	}

	loadPriceData(hpdFilename)

	transactionsFilename := flag.Arg(0)
	outputFile := flag.Arg(1)

	transactions := readTransactions(transactionsFilename)

	convertedTransactions := convertTransactions(transactions)

	writeConvertedTransactions(outputFile, convertedTransactions)

	// TODO-price-lookup storeHistoricalPriceCache(hpcFilename)
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
	expectedFirstRow := []string{"txid", "refid", "time", "type", "subtype", "aclass", "asset", "wallet", "amount", "fee", "balance"}

	firstRow := transactions[0]

	if !testSlicesEqual(firstRow, expectedFirstRow) {
		log.Fatalf("First row fails to match")
	}

	output := make(map[string][][]string, 0)
	pendingSpends := make(map[string]ledger)
	pendingSpotToStaking := make(map[string]ledger)
	pendingWithdrawals := make(map[string]ledger) // Only used by "withdrawal" transactions, which are not currently (2024-11) active
	pendingStakingDeposits := make(map[string]ledger)
	pendingTokenDeposits := make(map[string]ledger)

	// Coin values are found by asking CoinGecko for historical market data: a single API call can return N days worth of data.
	// Calculate how far back to go by finding the oldest entry in the transaction data - which happens to be the first record as it is
	// presented in forward data order - and add a margin of 10 days for safety.

	// TODO-price-lookup oldest_date, _ := time.Parse("2006-01-02 15:04:05", transactions[1][2])
	// TODO-price-lookup today := time.Now()
	// TODO-price-lookup days_ago := int(today.Sub(oldest_date).Hours()/24) + 10 // Add 10 to be sure that all data for necessary dates are available
	// fmt.Println("Oldest date/time: ", oldest_date, " now: ", today, " days-between", days_ago)
	// TODO-price-lookup SetDaysOfPriceHistoryToRequest(days_ago)

	for i, row := range transactions[1:] {
		csvRowIndex := i + 2
		entry := ledger{csvRowIndex, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]}

		ukTime := convertKrakenTimeToUKTime(entry.time)
		rowValuesAcceptable := areRowValuesAcceptable(entry)

		switch entry.format {
		case "spend":
			// This entry only occurs when a token is purchased by selling another token.
			// The "spend" entry covers selling the first token.
			// The corresponding "receive" entry covers buying the second token.
			// In all cases seen so far, the "spend" precedes the "receive".
			// The two are linked as a single logical transaction by having the same unique ref-id.
			if prev, found := pendingSpends[entry.refid]; found {
				fmt.Printf("Saw spend with repeated refid: %s (previous in row %d)\n", entry.refid, prev.row)
			}
			// Check txid not blank and format is valid
			// Check subtype is blank
			// Check that balance is not blank
			// This will be re-checked later but report it now in case no correspdonding "receive" is seen
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
				// totalSpendGBP := calculateSpendAsString(spend)
				totalSpendUSD := ""
				tokenPriceUSD := ""
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
					valid = false
				}
				// Handle a non-GBP spend; for now only FLOW, BTC, ETHW and DOT are handled
				// TODO: Note that entry.asset may well be XXBT instead of BTC; leave that for now
				note := fmt.Sprintf("SELL %s %s to buy %s %s", strings.TrimLeft(spend.amount, "-"), spend.asset, entry.amount, entry.asset)
				if spend.asset == "FLOW" || spend.asset == "DOT" || spend.asset == "ETHW" || spend.asset == "BTC" {
					// TODO here sell FLOW amount will be -ve to show a spend; there will be a matching refid to show the currency purchased
					// The spend in fiat currency is not known, so both the SELL and BUY will have to be calculated manually
					// As a starting point, find the value of the purchased currentcy and use that for both.
					// That should produce a reasonable value for the amount received for the initial token minus costs
					tokenValueFloat32, _ := LookupHistoricalTokenValue(entry.asset, entry.time)
					amount, err := strconv.ParseFloat(entry.amount, 32)
					if err != nil {
						fmt.Printf("Saw invalid token amount (%s) on row %d\n", entry.amount, entry.row)
						valid = false
					}
					tokenPriceUSD = fmt.Sprintf("%f", tokenValueFloat32)
					totalSpendUSD = fmt.Sprintf("%f", tokenValueFloat32*float32(amount))
					ukSpendTime := convertKrakenTimeToUKTime(spend.time)
					if valid {
						data := []string{"", "Kraken", spend.time, ukSpendTime, spend.amount, "", totalSpendUSD, "", "", "", "", "", "", "SELL", "", "", "", "", "", "", "", "", "", note}
						output[spend.asset] = append(output[spend.asset], data)
					} else {
						data := []string{"", "Kraken", spend.time, ukSpendTime, spend.amount, "", totalSpendUSD, "", "", "", "", "", "", "SELL ** BAD DATA**"}
						output[spend.asset] = append(output[spend.asset], data)

					}
				} else if spend.asset != "ZGBP" && entry.asset != "USD" {
					// TODO: consider both purchase of crypto using GBP and sale of crypto receiving GBP
					fmt.Printf("Saw non GBP (currency %s) 'spend' in row %d and currency %s 'receive' in row %d\n", spend.asset, spend.row, entry.asset, entry.row)
					valid = false
				}
				if valid {
					data := []string{"", "Kraken", entry.time, ukTime, entry.amount, tokenPriceUSD, totalSpendUSD, "", "", "", "", "", "", "BUY", "", "", "", "", "", "", "", "", "", note}
					output[entry.asset] = append(output[entry.asset], data)
				} else {
					data := []string{"**BAD DATA**", "Kraken", entry.time, ukTime, entry.amount, tokenPriceUSD, totalSpendUSD, "", "", "", "", "", "", "", "BUY **BAD DATA**"}
					output[entry.asset] = append(output[entry.asset], data)
				}
				// Remove the "spend" entry that has now been used
				delete(pendingSpends, entry.refid)
			}
		case "staking":
			// TBD: ensure that this code checks everything that is documented
			// TODO tidy up but otherwise all is complete
			// TODO expect wallet "spot / main"
			// expect subtype is blank
			// asset should have a suffix of .S
			// fee should be 0
			valid := rowValuesAcceptable
			stakedCurrency := strings.TrimSuffix(entry.asset, ".S")
			// TODO-find-meaning-of-.S if stakedCurrency == entry.asset {
			// TODO-find-meaning-of-.S 	valid = false
			// TODO-find-meaning-of-.S 	fmt.Printf("row %d, staking asset does not have .S suffix: %s\n [%s]\n", csvRowIndex, entry.asset, row)
			// TODO-find-meaning-of-.S }
			// Look for a pending deposit that matches the currency and the amount and has a blank txid.
			// If such an entry is found, remove it from the pending deposits
			// TODO-VERIFY-OR-REMOVE foundDeposit := false
			// TODO-VERIFY-OR-REMOVE for k, v := range pendingStakingDeposits {
			// TODO-VERIFY-OR-REMOVE	if v.asset == entry.asset && v.amount == entry.amount && v.txid == "" {
			// TODO-VERIFY-OR-REMOVE		delete(pendingStakingDeposits, k)
			// TODO-VERIFY-OR-REMOVE		foundDeposit = true
			// TODO-VERIFY-OR-REMOVE		break
			// TODO-VERIFY-OR-REMOVE	}
			// TODO-VERIFY-OR-REMOVE }
			// TODO-VERIFY-OR-REMOVE if !foundDeposit {
			// TODO-VERIFY-OR-REMOVE	fmt.Printf("Failed to find corresponding deposit for staking on row %d\n", entry.row)
			// TODO-VERIFY-OR-REMOVE }
			if valid {
				tokenValueFloat32, err := LookupHistoricalTokenValue(stakedCurrency, entry.time)
				if err != nil {
					log.Fatal(err)
				}
				tokenValue := fmt.Sprintf("%f", tokenValueFloat32)
				data := []string{"", "Kraken", entry.time, ukTime, entry.amount, tokenValue, "", "", "", "", "", "", "", "STAKING"}
				output[stakedCurrency] = append(output[stakedCurrency], data)
			} else {
				data := []string{"**BAD DATA**", "Kraken", entry.time, ukTime, entry.amount, "", "", "", "", "", "", "", "", "STAKING **BAD DATA**"}
				output[stakedCurrency] = append(output[stakedCurrency], data)
			}
		case "transfer":
			// TODO
			// "LWZJC4-FLL6I-VGTGBJ","BUMTXMY-2XXW32-Z6F6ME","2023-04-17 09:47:16","transfer","spottostaking","currency","FLOW","spot / main",-13000.0000000000,0,6041.7838117000
			// "LL722W-QEXKS-NOD242","RUL64FH-ONZZLW-PMDP5C","2023-04-17 09:47:58","transfer","stakingfromspot","currency","FLOW.S","spot / main",13000.0000000000,0,13000.0000008778

			// "LQW6OB-H6RUO-H5RKYU","FTFtciV-kUU09amveWZUx8DoaXWVmF","2023-11-02 17:50:21","transfer","spottostaking","currency","FLOW","spot / main",-6041.7838117000,0,0.0000000000
			// "LAOTPT-5Y6NM-ECKZE6","FTQFjkT-5mOIHj8nu4XWRyZDOI5zdA","2023-11-02 17:51:15","transfer","stakingfromspot","currency","FLOW.S","spot / main",6041.7838117000,0,19254.7259528278

			// "transfer" is used to move a cryptocurrency into a staking pool, so it never produces any output
			// TODO subtype must be either "spottostaking" or "stakingfromspot"
			// TOOD subtype "spottostaking" must be matched with a pending withdrawal
			// TODO subtype "stakingfromspot" must be matched with a pending staking deposit
			// TODO subtype "spotfromfutures" must be matched with a pending token deposit
			// TODO txid must not be blank
			// TODO balance must not be blank
			// TODO: may be matched with a previous "withdrawal", in which case it represents an initial move into staking
			//
			// This code checks everything that is documented.
			// In addition a transfer with subtype "spotfromfutures" (that requires a deposit with a matching refid) has been seen.
			// This happened during the Ethereum Merge (moving from PoW to PoS) and shows in the online history as "EthereumPoW".
			// It has been noted and checked, but no output is generated,
			if entry.subtype == "spottostaking" {
				// This entry (and the matching "stakingfromspot") represent a move of a cryptoasset to the staking pool
				// It is assumed that the "spottostaking" will always precede the matching "stakingfromspot".
				// There seems to be no connection, the ref-id values do not match.
				// It seems that the only way to match "spottostaking" and "stakingfromspot" is to check the amounts and require that the two transactions be within a few seconds of each other!
				// TOOD: For now, just ignore this entry.
				// No output row will be written.
			} else if entry.subtype == "stakingfromspot" {
				// This entry (and the matching "spottostaking") represent a move of a cryptoasset to the staking pool
				// It is assumed that the "spottostaking" will always precede the matching "stakingfromspot".
				// There seems to be no connection, the ref-id values do not match.
				// It seems that the only way to match "spottostaking" and "stakingfromspot" is to check the amounts and require that the two transactions be within a few seconds of each other!
				// TOOD: For now, just ignore this entry.
				// No output row will be written.
			} else if entry.subtype == "spotfromfutures" {
				// Since at least late 2024 this transaction type may have changed and so may no longer be handled correctly.
				// Do not remove the log.fatal() without verifying transaction handling and correcting if necessary.
				log.Fatalf("row %d: unhandled transaction type %s", entry.row, entry.format)

				if _, found := pendingTokenDeposits[entry.refid]; !found {
					fmt.Printf("transfer spotfromfutures with no matching deposit on row %d\n", entry.row)
				} else {
					delete(pendingTokenDeposits, entry.refid)
				}
			} else if entry.subtype == "stakingtospot" {
				// Since at least late 2024 this transaction type may have changed and so may no longer be handled correctly.
				// Do not remove the log.fatal() without verifying transaction handling and correcting if necessary.
				log.Fatalf("row %d: unhandled transaction type %s", entry.row, entry.format)

				// This seems to represent a withdrawl from staking
				// TODO this should match a withdrawl and should involve a .S currency
				valid := true
				var withdrawal ledger
				withdrawal, valid = pendingSpotToStaking[entry.refid]
				if !valid {
					fmt.Printf("transfer (stakingtospot) on row %d has no matching withdrawal\n", entry.row)
				} else if (entry.amount != withdrawal.amount) || (entry.fee != withdrawal.fee) || (entry.asset != withdrawal.asset) {
					fmt.Printf("transfer (stakingtospot) on row %d does not properly match withdrawal on row %d\n", entry.row, withdrawal.row)
				}
				delete(pendingSpotToStaking, entry.refid)
			} else if entry.subtype == "spotfromstaking" {
				// Since at least late 2024 this transaction type may have changed and so may no longer be handled correctly.
				// Do not remove the log.fatal() without verifying transaction handling and correcting if necessary.
				log.Fatalf("row %d: unhandled transaction type %s", entry.row, entry.format)

				// This seems to represent a withdrawl from staking
				// TODO should match a deposit, but there is no check for that yet
			} else {
				// Since at least late 2024 this transaction type may have changed and so may no longer be handled correctly.
				// Do not remove the log.fatal() without verifying transaction handling and correcting if necessary.
				log.Fatalf("row %d: unhandled transaction type %s", entry.row, entry.format)

				fmt.Printf("Invalid subtype (%s) for transfer on row %d\n", entry.subtype, entry.row)
			}
		case "earn":
			// "earn/migration" seems to occur as a pair with spot and earn wallets.
			// It seems to be a new way of recording staking.
			// "earn/reward" seems to be just like staking
			valid := rowValuesAcceptable
			if entry.subtype == "migration" {
				// TODO there should be some checks to make sure that ear/migrations match
				// TODO for now, ignore this as there is no output and no tax implication
			} else if entry.subtype == "reward" {
				stakedCurrency := entry.asset
				if valid {
					tokenValueFloat32, err := LookupHistoricalTokenValue(stakedCurrency, entry.time)
					if err != nil {
						log.Fatal(err)
					}
					tokenValue := fmt.Sprintf("%f", tokenValueFloat32)
					data := []string{"", "Kraken", entry.time, ukTime, entry.amount, tokenValue, "", "", "", "", "", "", "", "STAKING"}
					output[stakedCurrency] = append(output[stakedCurrency], data)
				} else {
					data := []string{"**BAD DATA**", "Kraken", entry.time, ukTime, entry.amount, "", "", "", "", "", "", "", "", "STAKING **BAD DATA**"}
					output[stakedCurrency] = append(output[stakedCurrency], data)
				}
			} else if entry.subtype == "deallocation" {
				// Deallocation doesn't seem to record anything meaningful, so ignore it
			} else {
				log.Fatalf("row %d: unhandled earn subtype %s\n", entry.row, entry.subtype)
			}
			// "LU4CV4-NPA3Y-BSYGR6","ELJ5SEV-RRMHX-BY3FP6","2024-02-15 12:32:39","earn","migration","currency","FLOW.S","spot / main",-19362.3315756678,0,0.0000000000
			// "LZNGUJ-GH5K5-M2UR5R","ELJ5SEV-RRMHX-BY3FP6","2024-02-15 12:32:39","earn","migration","currency","FLOW","earn / flexible",19362.3315756678,0,19362.3315756678
			// "LAOAXD-FRAPU-63S5C6","ELRVG7C-I7Y3J-UXVADZ","2024-02-16 13:30:33","earn","reward","currency","FLOW","earn / flexible",1.8798328066,0.3759665613,19363.8354419131
			// TBD: ensure that this code checks everything that is documented
			// TODO tidy up but otherwise all is complete
			// TODO expect wallet "spot / main"
			// expect subtype is blank
			// asset should have a suffix of .S
			// fee should be 0

			// TODO-find-meaning-of-.S if stakedCurrency == entry.asset {
			// TODO-find-meaning-of-.S 	valid = false
			// TODO-find-meaning-of-.S 	fmt.Printf("row %d, staking asset does not have .S suffix: %s\n [%s]\n", csvRowIndex, entry.asset, row)
			// TODO-find-meaning-of-.S }
			// Look for a pending deposit that matches the currency and the amount and has a blank txid.
			// If such an entry is found, remove it from the pending deposits
			// TODO-VERIFY-OR-REMOVE foundDeposit := false
			// TODO-VERIFY-OR-REMOVE for k, v := range pendingStakingDeposits {
			// TODO-VERIFY-OR-REMOVE	if v.asset == entry.asset && v.amount == entry.amount && v.txid == "" {
			// TODO-VERIFY-OR-REMOVE		delete(pendingStakingDeposits, k)
			// TODO-VERIFY-OR-REMOVE		foundDeposit = true
			// TODO-VERIFY-OR-REMOVE		break
			// TODO-VERIFY-OR-REMOVE	}
			// TODO-VERIFY-OR-REMOVE }
			// TODO-VERIFY-OR-REMOVE if !foundDeposit {
			// TODO-VERIFY-OR-REMOVE	fmt.Printf("Failed to find corresponding deposit for staking on row %d\n", entry.row)
			// TODO-VERIFY-OR-REMOVE }

		case "deposit":
			// In early 2025 "deposit" is seen when a crypto currency is transferred to the exchange.
			// So sending ETH from a wallet to Kraken would result in a "deposit" record.
			// That should result in either a TRANSFER-IN or a MOVE-IN depending on the identity of the sender.
			// The code assumes a TRANSFER-IN: if it is a MOVE-IN the manual adjustment will be needed

			// TBD: ensure that this code checks everything that is documented
			// If fiat currency, then it can be ignored (only "ZGBP" or "ZEUR" or "EUR.HOLD" will be seen here).
			// If the currency ends in ".S" then this is the staking version of the currency, so save it to later match against a "transfer".
			// Otherwise this is a TRANSFER-IN of that currency.
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
				data := []string{"", "Kraken", entry.time, ukTime, entry.amount, "", "", "", "", "", "", "", "", "TRANSFER-IN"}
				output[entry.asset] = append(output[entry.asset], data)
			}
		case "withdrawal":
			// Since at least late 2024 this transaction type may have changed and so may no longer be handled correctly.
			// Do not remove the log.fatal() without verifying transaction handling and correcting if necessary.
			log.Fatalf("row %d: unhandled transaction type %s", entry.row, entry.format)

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
					data := []string{"**BAD DATA**", "Kraken", entry.time, ukTime, entry.amount, "", "", "", "", "", "", "", "", "TRANSFER-OUT **BAD DATA**"}
					output[entry.asset] = append(output[entry.asset], data)
				}
				delete(pendingWithdrawals, entry.refid)
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
	for _, v := range pendingSpotToStaking {
		fmt.Printf("Error: Unmatched \"spotTostaking\": row: %d entry=%v\n", v.row, v)
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
//   - the number of elements is identical
//   - the corresponding elements match exactly
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

	if entry.subtype != "" && (entry.format != "earn" && entry.format != "transfer") {
		fmt.Printf("ledger entry row %d has non-blank subtype\n", entry.row)
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

// This will be a map of '"coin-name" @ "YYYY-MM-DD"' => coin price in USD as a fp number
var coinHistoricalPrices map[string]float32

func loadPriceData(priceDataDir string) {

	coins := []string{
		"ada",
		"avax",
		"axs",
		"bsgg",
		"btc",
		"dot",
		"doge",
		"enj",
		"erg",
		"eth",
		"flow",
		"mana",
		"sand",
		"sol",
		"wmemo",
	}

	coinHistoricalPrices = make(map[string]float32)
	for _, c := range coins {
		priceFile := fmt.Sprintf("%scoin-price-data-%s-usd.csv", priceDataDir, c)
		// DEBUG fmt.Println(index, priceFile)
		loadPriceDataForCoin(c, priceFile)
	}

}

func loadPriceDataForCoin(c string, priceFile string) {
	f, err := os.Open(priceFile)
	if err != nil {
		log.Fatalf("Unable to read input price file %s", priceFile, err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	data, err := csvReader.ReadAll()
	if err != nil {
		log.Fatalf("Unable to parse file as CSV for %s", priceFile, err)
	}
	for line_num, entry := range data {
		if line_num == 0 {
			continue
		}
		date, err := time.Parse("2006-01-02 15:04:05 UTC", entry[0])
		if err != nil {
			log.Fatalf("Invalid date (%s) on line %d of file %s\n", entry[0], line_num, priceFile, err)
		}
		cacheDate := date.Format("2006-01-02")
		price64, err := strconv.ParseFloat(entry[1], 32)
		if err != nil {
			// do something sensible
		}
		price := float32(price64)
		cacheIndex := c + "@" + cacheDate
		_, ok := coinHistoricalPrices[cacheIndex]
		// If the key exists already, something is wrong
		if ok {
			log.Fatalf("duplicate cache value (index=%s) at line %d in file %s", cacheIndex, line_num, priceFile)
		}
		coinHistoricalPrices[cacheIndex] = price
	}

}

func LookupHistoricalTokenValue(requestedToken string, dateTime string) (float32, error) {
	// TODO This function might be called with USD ... suppress the error in that case
	if strings.ToLower(requestedToken) == "usd" {
		return -1.0, nil
	}
	// Verify the date is valid and turn into the format coingecko wants (DD-MM-YY HH:MM:SS)
	date, err := time.Parse("2006-01-02 15:04:05", dateTime)
	if err != nil {
		log.Fatalf("Invalid date (%s) when looking up token %s: %s\n", dateTime, requestedToken, err)
	}
	cgDate := date.Format("2006-01-02")

	// The cache is indexed according to coin name and date
	index := strings.ToLower(requestedToken) + "@" + cgDate
	price, ok := coinHistoricalPrices[index]
	// If the key exists
	if !ok {
		fmt.Printf("failed to find price for [%s]\n", index)
		price = -1.0
	}

	// Deliberately return no error for now ... no prices are available yet!
	// DEBUG fmt.Printf("Price lookup for %s at %s produces %f\n", requestedToken, dateTime, price)
	return price, nil

}
