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

from config import apikey

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
    """Return today's date."""
    return datetime.date.today().strftime("%Y-%m-%d")


def save_to_csv(data, save_directory, table):
    """
    Saves the dataframes to csv format:
    :param data: Dataframe downloaded from Quandl.
    :param save_directory: Directory where csv's will be saved.
    :param table: Table name downloaded and being saved. e.g. 'SEP'
    :return None:
    """
    path = Path(save_directory)
    path.mkdir(parents=True, exist_ok=True)

    file_name = table + ".csv"
    file_path = path / file_name
    data.to_csv(file_path, index=False)

    return None


def connect(save_directory, db):
    """
    Creating a connection to a database. If doesn't exist, then warning and create the database.
    :param save_directory: Directory where the database will be housed.
    :param db: Database name.
    :return conn: Returns a connection object.
    """
    path = Path(save_directory)
    filepath = path / db

    # Connect to the database.
    return sqlite3.connect(filepath)


def get_data(table, kwarg):
    """
    Using the table name and kwargs retrieves the most current data.
    :param table: Name of table to update.
    :param kwarg: Dictionary containing the parameters to send to Quandl.
    :return dataframe: Pandas dataframe containing latest data for the table.
    """
    return quandl.get_table("SHARADAR/" + table.upper(), paginate=True, **kwarg)


def store_data(table, args):
    """
    Evaluates the current state of stored data, identifies where the latest rows in the table are,
    and then seeks out new data from Sharadar, then saves this downloaded data to the table.
    :param table: String identifying which table to save.
    :param args: argsparse object.
    :return None: Nothing to return the module updates a database.
    """
    # Retrieve the new data from Quandl
    print("Getting data, updating {}".format(table))

    end_date = get_today()

    # Connect to the database.
    conn = connect(args.save_directory, args.db)
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
        df_new_data = get_data(table, kwarg)
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


def main(args=None):
    """
    Entry point. Manages user requests and downloads Sharadar data, displays, and saves data
    creates csv files and database.
    Use update.py --help to get  help for parameters.
    :param args:
    :return None:
    """

    args = parse_args(args)

    # api_key = args.key    ### todo USE THIS LINE IN LIVE VERSION.
    api_key = apikey  ### todo delete this
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
            store_data(t, args)
    elif args.fromdate and args.todate:
        for t in tables:
            # Retrieve data.
            kwarg = {sharadar_tables[t][0]: {"gte": args.fromdate, "lte": args.todate}}
            data = get_data(t, kwarg)

            # Save files
            if bool(int(args.csv)):
                save_to_csv(data, args.save_directory, t)
            else:
                pass

            # Print results.
            pd.options.display.max_rows = int(args.display_rows)
            if bool(int(args.printon)):
                print("{} - {}".format(t, sharadar_tables[t][1]))
                print(data.head(int(args.display_rows)))
            else:
                pass

    return None


def add_bool_arg(parser, name, default=False, help_text=""):
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--" + name, dest=name, action="store_true", help=help_text)
    group.add_argument("--no-" + name, dest=name, action="store_false", help=None)
    parser.set_defaults(**{name: default})


# noinspection PyTypeChecker
def parse_args(pargs=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=("Module for downloading Sharadar data from Quandl."),
    )

    parser.add_argument(
        "--tables",
        nargs="+",
        required=False,
        default="",
        help="Tables to be downloaded. If blank all tables will be downloaded. Tables are: SF1 SF2 SF3 EVENTS "
        "SF3A SF3B SEP TICKERS INDICATORS DAILY SP500 ACTIONS SFP",
    )

    add_bool_arg(
        parser,
        "update",
        False,
        "Update the downloaded data to database tables. --update True; --no-update False",
    )

    parser.add_argument(
        "--fromdate", required=False, default="", help="Date to download data from.",
    )

    parser.add_argument(
        "--todate", required=False, default="", help="Date to download data to.",
    )

    add_bool_arg(parser, "db", False, "Save to database. --db True; --no-db False")

    add_bool_arg(parser, "csv", False, "Save to csv file. --csv True; --no-csv False")

    parser.add_argument(
        "--save_directory",
        required=False,
        default="data",
        help="Sub directory to save the csv files in.",
    )

    add_bool_arg(
        parser,
        "printon",
        True,
        "Print to terminal the data downloaded. --printon True; --no-printon False",
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
        "--key", required=False, default="your_api_key", help="Quandl API key.",
    )  # todo remove apikey
    return parser.parse_args(pargs)


if __name__ == "__main__":

    main()
