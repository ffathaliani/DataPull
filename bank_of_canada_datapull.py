import csv
import argparse
import os
import urllib.request
import json
from datetime import datetime, timedelta, date

# parameters to set
# --CurrencyRateReport --Day=60 --OutputFile="boc-currency-exchange-rate.csv" --OutputLocation="/Users/Ramin/Desktop/BankOfCanada"

currencies = {"FXAUDCAD": "AUD", "FXBRLCAD": "BRL", "FXCNYCAD": "CNY", "FXEURCAD": "EUR", "FXHKDCAD": "HKD",
              "FXINRCAD": "INR", "FXIDRCAD": "IDR", "FXJPYCAD": "JPY", "FXMYRCAD": "MYR", "FXMXNCAD": "MXN",
              "FXNZDCAD": "NZD", "FXNOKCAD": "NOK", "FXPENCAD": "PEN", "FXRUBCAD": "RUB", "FXSARCAD": "SAR",
              "FXSGDCAD": "SGD", "FXZARCAD": "ZAR", "FXKRWCAD": "KRW", "FXSEKCAD": "SEK", "FXCHFCAD": "CHF",
              "FXTWDCAD": "TWD", "FXTHBCAD": "THB", "FXTRYCAD": "TRY", "FXGBPCAD": "GBP", "FXUSDCAD": "USD",
              "FXVNDCAD": "VND"}

def main():
    #The first step in using the argparse is creating an ArgumentParser object:
    parser = argparse.ArgumentParser(description="Export data from Bank Of Canada API")
    parser.add_argument("--CurrencyRateReport",
                        help="Export all currency exchange rates from BOC, by day",
                        action='store_true')
    parser.add_argument("--Day",
                        help="Days to reports",
                        type=int,
                        default=30)
    parser.add_argument("--OutputFile", help="Output file name to write the report data. (csv format)",
                        default="boc-currency-exchange-rate-original.csv")
    parser.add_argument("--OutputLocation", help="Output file location", default="/Users/Ramin/Desktop/BankOfCanada")
    args = parser.parse_args()


    OutputFile = os.path.join(args.OutputLocation, args.OutputFile)
    dayToPull = args.Day

    print('Pulling %d day in the past in file "%s"' % (dayToPull, OutputFile))

    if args.CurrencyRateReport:
        exportCurrencyRatesReport(dayToPull, OutputFile)
    else:
        print("No report selected")


def exportCurrencyRatesReport(daysToGet, OutputFile):
    print('Start: exportCurrencyRatesReport')

    startDate = date.today() - timedelta(days=daysToGet)
    endDate = date.today() - timedelta(days=1)

    if startDate < date(2017, 5, 1): # Before 2017-05-01 Legacy data is used
        startDate = date(2017, 5, 1)

    query_parameters = {'start_date': startDate, 'end_date': endDate}

    url_currencies = ','.join(currencies.keys())

    encoded_query_parameters = urllib.parse.urlencode(query_parameters)

    print("This is encoded_query_parameters:" + encoded_query_parameters)

    with urllib.request.urlopen(
                    'http://www.bankofcanada.ca/valet/observations/'+ url_currencies + '/json?' + encoded_query_parameters) as url:

        data = json.loads(url.read().decode())

    writable_data = data.get('observations')
    writtenRate=0

    with open(OutputFile, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['Date', 'Currency Code', 'Rate'])

        for item in writable_data:
            date_to_report = item["d"]
            for cur in currencies.keys():
                try:
                    valid_curr = item[cur]
                    try:
                        valid_rate = valid_curr["v"]
                        writer.writerow([date_to_report, currencies[cur], '{:f}'.format(valid_rate)])
                        writtenRate+=1
                    except KeyError as error:
                        print("Warning:", date_to_report, cur, "reported invalid rate value")
                except KeyError as e:
                    print("Warning:", date_to_report, cur, "Currency missing from data")
        print('Exported %d rates' % writtenRate)

    print('End: exportCurrencyRatesReport')


if __name__ == '__main__':
    main()
