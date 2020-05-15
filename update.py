###############################################################################
#
# Software program written by Neil Murphy in year 2020.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# By using this software, the Disclaimer and Terms distributed with the
# software are deemed accepted, without limitation, by user.
#
# You should have received a copy of the Disclaimer and Terms document
# along with this program.  If not, see... https://bit.ly/2Tlr9ii
#
###############################################################################
from pathlib import Path
import argparse
import sys
import datetime

import pandas as pd
import quandl
import sqlite3

"""
For downloading zip files. 
https://www.quandl.com/tables/SHARADAR-SF1/export?api_key=insert_key
"""

sharadar_tables = {
    "SF1": ["lastupdated", "Core US Fndamentals"],
    "DAILY": ["lastupdated", "Daily Metrics"],
    "SEP": ["date", "Sharadar Equity Prices"],
    "TICKERS": ["lastupdated", "Tickers and Metadata"],
    "ACTIONS": ["date", "Corporate Actions"],
    "EVENTS": ["date", "Core US Fundamental Events"],
    "SF2": ["filingdate", "Core US Insiders"],
    # "SF3": ["calendardate", "Core US Institutional Investors Summary by Ticker"],
    "SF3A": ["calendardate", "Core US Institutional Investors Summary by Ticker"],
    "SF3B": ["calendardate", "Core US Institutional Investors Summary by Investor"],
    "SFP": ["date", "Sharadar Fund Prices"],
    "SP500": ["date", "S&P500 Current and Historical Constituents"],
}


def get_today():
    # Get today's date.
    return datetime.date.today().strftime("%Y-%m-%d")


def save_to_csv(data, save_dir):
    """
    Saves all the dataframes to csv format:
    :param data: Dictionary with keys with table names, values with dataframes.
    :param save_dir: Directory where csv's will be saved.
    :return None:
    """
    path = Path(save_dir)
    path.mkdir(parents=True, exist_ok=True)

    for table_name, dataframe in data.items():
        file_name = table_name + ".csv"
        file_path = path / file_name
        dataframe.to_csv(file_path)

    return None


def store_data(table, sharadar_tables):
    # Retrieve the new data from Quandl
    print("Getting data, updating {}".format(table))

    end_date = get_today()

    # Connect to the database.
    conn = sqlite3.connect("data/sharadar.db")
    c = conn.cursor()

    sql = "SELECT MAX({}) FROM {}".format(sharadar_tables[table][0], table)
    c.execute(sql)
    start_date = (pd.to_datetime(c.fetchone()[0]) + pd.Timedelta("1 days")).strftime(
        "%Y-%m-%d"
    )

    # Exit if the start date equal today.
    if start_date == end_date:
        print(
            "Update from Quandl for {} cancelled since the start date is the same as the end date.".format(
                table
            )
        )
        return None
    else:
        kwarg = {sharadar_tables[table][0]: {"gte": start_date, "lte": end_date}}
        df_new_data = quandl.get_table(
            "SHARADAR/" + table.upper(), paginate=True, **kwarg
        )
    if df_new_data.shape[0] == 0:
        print("No new data for {}".format(table))
        return
    else:
        pass

    print(df_new_data.head())
    print(df_new_data.tail())

    # Join the new data from Quandl with the existing dataframe and sort, reset index.
    df_new_data.to_sql(table, con=conn, if_exists="append", index=False)

    print(
        "Successfully updated {} with {} rows from {} to {}".format(
            table, df_new_data.shape[0], start_date, end_date
        )
    )

    return None


def get_table(sharadar_tables, args=None):
    """
    Download each sharadar table
    :param tables; List with the tables to be downloaded.
    :return dictionary: dictionary with keys are table names, and values are dataframes.
    """

    args = parse_args(args)

    # api_key = args.key    ### USE THIS LINE IN LIVE VERSION.
    filepath = 'key.txt'
    with open(filepath) as fp:
        api_key = fp.readline()
    quandl.ApiConfig.api_key = api_key

    # Download dataframes from quandl. If argument provided ALL will give all tables.
    # Caution as this will be a lot of memory.
    if args.tables:
        if args.tables == ["ALL"]:
            print(
                "Downloading all of the SHARADAR tables at the same time will take  up a lot of memory. \n"
                " If you wish to continues please type 'ALL' again."
            )
            confirmed = input()

            if confirmed == "ALL":
                tables = sharadar_tables.keys()
            else:
                sys.exit()
        else:
            tables = args.tables
    else:
        tables = ["DAILY"]

    if args.update:
        # Open tables, get data, and save.
        for t in tables:
            store_data(t, sharadar_tables)
    elif args.fromdate and args.todate:
        data = {}
        for t in tables:
            kwarg = {sharadar_tables[t][0]: {"gte": args.fromdate, "lte": args.todate}}
            data[t] = quandl.get_table(args.dataset + "/" + t, paginate=True, **kwarg)
        # Save files
        if bool(int(args.csv)):
            save_to_csv(data, args.save_directory)
        else:
            pass

        # Print results.
        pd.options.display.max_rows = int(args.display_rows)
        if bool(int(args.printon)):
            for k, v in data.items():
                print("{} - {}".format(k, sharadar_tables[k][1]))
                print(v.head(int(args.display_rows)))
        else:
            pass

    return None


def parse_args(pargs=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=("Andrew Wert downloading Quandl data."),
    )


    parser.add_argument(
        "--tables",
        nargs="+",
        required=False,
        default="",
        help="Tables to be downloaded. If blank all tables will be downloaded. Tables are: SF1 SF2 SF3 EVENTS "
        "SF3A SF3B SEP TICKERS INDICATORS DAILY SP500 ACTIONS SFP",
    )

    parser.add_argument(
        "--update",
        required=False,
        default=0,
        help="Update the downloaded data to sqlite3 database tables.",
    )

    parser.add_argument(
        "--fromdate", required=False, default="", help="Date to download data from.",
    )

    parser.add_argument(
        "--todate", required=False, default="", help="Date to download data to.",
    )

    parser.add_argument(
        "--csv",
        required=False,
        default="0",
        help="Save to csv file. If 1 then results will be saved to csv, 0 to not "
        "save to csv.",
    )

    parser.add_argument(
        "--save_directory",
        required=False,
        default="data",
        help="Sub directory to save the csv files in.",
    )

    parser.add_argument(
        "--printon",
        required=False,
        default="1",
        help="Print to terminal the data downloaded. 1 for on, 0 for off",
    )

    parser.add_argument(
        "--display_rows",
        required=False,
        default="5",
        help="How many rows to display when printing.",
    )

    parser.add_argument(
        "--dataset",
        required=False,
        default="SHARADAR",
        help="Name of the dataset. Do not adjust.",
    )

    parser.add_argument(
        "--key", required=False, default="your_temp_key", help="Quandl API key.",
    )
    return parser.parse_args(pargs)


if __name__ == "__main__":

    get_table(sharadar_tables)
