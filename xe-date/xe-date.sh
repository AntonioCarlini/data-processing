#!/usr/bin/env bash

# Fetch the historical USD to GBP exchange rate for a range of dates.

# Invoke with either a single date (to fetch the value on that date only) or a start date
# and an end date. The dates must be in the format YYYY-MM-DD.
# So:
# Fetch the USD->GBP exchange rate for 1st Feb 2022
# ./xe-date.sh 2022-02-01
#
# Fetch the USD->GBP exchange rate for every day from 1st May 2021 until 10th Sep 2021 (inclusive)
# ./xe-date.sh 2021-05-01 2021-09-10 # 

# This function accepts a date in YYYY-MM-DD format and fetches a table that contains the
# required exchange value. It then findx the single line that contains the required value
# and uses sed to extract just the USD=>GBP exchange rate as a number.
# The result is written to stdout as "date,,value", so that a CSV file of successive data
# can be built up.
fetch_and_display_usd_gbp_for_date() {
    gbp=$(wget  -q -O - "https://www.xe.com/currencytables/?from=USD&date=$1#table-section" |
    grep -E 'GBP<\/a><\/th><td>British Pound<\/td><td>[[:digit:]]+' |
    sed -rn 's/(.*)(GBP<\/a><\/th><td>British Pound<\/td><td>)([\.[:digit:]]+)(.*)/\3/p')
    echo "$1,,${gbp}"
}


# The main entry point.
# Validates the input date (or dates) and calls the function that preforms a lookup for each date.
# The results are written to stdout as "date,exchange-rate" in CSV format.
main() {

    if [[ $# -lt 1 || $# -gt 2 ]]; then
        echo "Supply a start date OR a start date and an end date, both in YYYY-MM-DD format."
        exit 1
    fi
    
    start_date=""
    end_date=""

    error=0

    req_start_date=$1

    if ! date "+%Y-%m-%d" -d "${req_start_date}" > /dev/null  2>&1; then
        echo "Invalid start date [${req_start_date}], please use the format YYYY-MM-DD"
        error=1
    else
        start_date=$(date "+%Y-%m-%d" -d "${req_start_date}")
        if [[ "${start_date}" != "${req_start_date}" ]]; then
            echo "Unrecognised start date ${req_start_date}, please use the format YYYY-MM-DD"
        fi
    fi

    if [[ $# -eq 2 ]]; then
        req_end_date=$2
        if ! date "+%Y-%m-%d" -d "${req_end_date}" > /dev/null  2>&1; then
            echo "Invalid start date [${req_end_date}], please use the format YYYY-MM-DD"
            error=1
        else
            end_date=$(date "+%Y-%m-%d" -d "${req_end_date}")
            if [[ "${end_date}" != "${req_end_date}" ]]; then
                echo "Unrecognised end date ${req_end_date}, please use the format YYYY-MM-DD"
            fi
        fi

        start_seconds=$(date "+%s" -d "${start_date}")
        end_seconds=$(date "+%s" -d "${end_date}")
        if [[ ${start_seconds} -gt ${end_seconds} ]]; then
            echo "Start date (${start_date}) must not be beyond end date (${end_date})"
            error=1
        fi
    fi

    # Exit now if any error has been seen up to this point.
    if [[ ${error} -ne 0 ]]; then
        echo "Fatal error encountered. Quitting."
        exit 2
    fi

    # Always perform a lookup for the start date
    fetch_and_display_usd_gbp_for_date "${start_date}"
    # Stop if no end date was specified (i.e. perform only one lookup for the start date).
    if [[ "${end_date}" == "" ]]; then
        exit 0
    fi

    next_date="${start_date}"
    until [[ "${next_date}" == "${end_date}" ]]
    do
        next_date=$(date "+%Y-%m-%d" -d "${next_date} next day")
        fetch_and_display_usd_gbp_for_date "${next_date}"
    done

    #fetch_date 2022-03-03

}

main "$@"
