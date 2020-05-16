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


def to_db(table, args):
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


def to_csv(table, args):
    """
    Saves the dataframes to csv format:
    :param data: Dataframe downloaded from Quandl.
    :param directory: Directory where csv's will be saved.
    :param table: Table name downloaded and being saved. e.g. 'SEP'
    :return None:
    """
    path = Path(args.directory)
    path.mkdir(parents=True, exist_ok=True)

    df_csv = pd.DataFrame()
    date_col = sharadar_tables[table][0]

    # Get ending date.
    if args.todate:
        date_end = args.todate
    else:
        date_end = get_today()

        # Get ending date.
    if args.fromdate:
        date_start = args.fromdate
    else:
        # Arbitrarily old start date will get data starting at the first row.
        date_start = "2000-01-01"

    # Set file path.
    file_name = args.save_name + "_" + table + ".csv"
    file_path = path / file_name

    # Open csv if exist to dataframe and get maximum date.
    if file_path.is_file():
        df_csv = pd.read_csv(file_path, parse_dates=[date_col], infer_datetime_format=True).sort_values(date_col)
        date_start = df_csv[date_col].max().strftime("%Y-%m-%d")
    else:
        pass

    # Check start and end dates.
    if date_end < date_start:
        raise ValueError("Your start date {} is after your end date {}".format(date_start, date_end))

    # Get the data between the dates from Quandl
    kwarg = {date_col: {"gte": date_start, "lte": date_end}}
    df_new = get_data(table, kwarg)
    df_new = df_new.sort_values(date_col)
    # df_new[date_col] = df_new[date_col].dt.date.astype('object')

    # Save data.
    if df_csv.size != 0:
        df_new = pd.concat([df_csv, df_new])
    else:
        pass

    df_new = df_new.sort_values('date')

    # df_new = df_new.sort_values(sharadar_tables[table][0])
    df_new.to_csv(file_path, index=False)

    return None


def sync_table(table, args):
    """
    Manages the downloading, saving, printing of a specific table.
    :param table: The Sharadar table to sync.
    :param args: Paramaters input by the user.
    :return None:
    """
    # Database
    if args.save_to == "db":
        print("databases {}".format(args.save_to))

    # CSV.
    elif args.save_to == "csv":
        to_csv(table, args)
        print("Table {} saved to csv file.".format(table))

    else:
        print(
            "{} not saved. If you wish to save please provide argument --save_to and either 'csv' or 'db'.".format(
                table
            )
        )

    # Terminal.
    if args.print_rows != 0:
        print("terminal {}".format(args.print_rows))
    else:
        pass


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

    # Download dataframes from quandl. Set the tables.

    if args.tables:
        tables = args.tables
        for t in tables:
            if t not in sharadar_tables.keys():
                raise ValueError(
                    "The table {} is not a valid table. Valid table names are: {}".format(
                        t, sharadar_tables.keys()
                    )
                )
    else:
        tables = sharadar_tables.keys()
    print("tables {}".format(tables))

    # Open tables, get data, and save.
    for t in tables:
        sync_table(t, args)
    #
    #
    # elif args.fromdate:
    #     if args.todate:
    #         todate = args.todate
    #     else:
    #         todate = get_today()
    #
    #     for t in tables:
    #         # Retrieve data.
    #         kwarg = {sharadar_tables[t][0]: {"gte": args.fromdate, "lte": todate}}
    #         data = get_data(t, kwarg)
    #
    #         # Save files
    #         if args.csv:
    #             save_to_csv(data, args.save_directory, t)
    #         else:
    #             pass
    #
    #         # Print results.
    #         pd.options.display.max_rows = int(args.display_rows)
    #         if args.print_rows != 0:
    #             print("{} - {}".format(t, sharadar_tables[t][1]))
    #             print(data.head(int(args.print_rows)))
    #         else:
    #             pass

    return None

# todo if not used by end.
def add_bool_arg(parser, name, default=False, help_text=""):
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--" + name, dest=name, action="store_true", help=help_text)
    group.add_argument("--no-" + name, dest=name, action="store_false", help=None)
    parser.set_defaults(**{name: default})


# noinspection PyTypeChecker
def parse_args(pargs=None):
    parser = argparse.ArgumentParser(
        # formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        formatter_class=argparse.RawTextHelpFormatter,
        description=("Module for downloading Sharadar data from Quandl."),
    )

    parser.add_argument(
        "--tables",
        nargs="+",
        required=False,
        default="",
        help="Tables to be downloaded. If blank all tables will be downloaded. \n"
        "Tables are: SF1 SF2 SF3 EVENTS SF3A SF3B SEP TICKERS INDICATORS DAILY SP500 ACTIONS SFP",
    )

    parser.add_argument(
        "--fromdate",
        required=False,
        default="",
        help="Date to download data from. If blank and table exists, then fromdate will be from the latest date in \n"
        "the table.  If blank and no table, will start downloading from the beginning of the dataset.",
    )

    parser.add_argument(
        "--todate",
        required=False,
        default="",
        help="Date to download data to. If blank, download to today's date.",
    )

    parser.add_argument(
        "--save_to",
        required=False,
        default="",
        help="Save to either database or csv. Valid values are db or csv. Default is None. \n"
        "If the database or csv does not exist, it will be created. \n"
        "If CSV exists and fromdate provided then csv file will be overwritten. \n"
        "If CSV exists and fromdate not provided, then csv will be updated to latest date. \n"
        "If database exist and table does not exists, table will be created. \n"
        "If database exist and table exist, and fromdate provided, table will be overwritten. \n"
        "If database exist and table exist, and fromdate not provided, table will be updated.\n",
    )

    parser.add_argument(
        "--save_name",
        required=False,
        default="",
        help="If provided will be the name of the database, or the pre-pend text to the csv. \n"
        "For example, if 'mydata' and csv table is DAILY, then file will be 'mydata_DAILY.csv'",
    )

    parser.add_argument(
        "--directory",
        required=False,
        default=".",
        help="Sub directory to save the csv or database files in.",
    )

    parser.add_argument(
        "--print_rows",
        required=False,
        default=0,
        help="Turn printing on to the terminal by indicating how many rows to display. \n"
        "Accepts integer as number of rows to display.",
    )

    parser.add_argument(
        "--key", required=False, default="your_api_key", help="Quandl API key.",
    )  # todo remove apikey

    return parser.parse_args(pargs)


if __name__ == "__main__":

    main()
