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
    "SF1": ["lastupdated", "Core US Fndamentals", 7],
    "DAILY": ["lastupdated", "Daily Metrics", 9],
    "SEP": ["date", "Sharadar Equity Prices", 11],
    "TICKERS": ["lastupdated", "Tickers and Metadata", 1],
    "ACTIONS": ["date", "Corporate Actions", 1],
    "EVENTS": ["date", "Core US Fundamental Events", 1],
    "SF2": ["filingdate", "Core US Insiders", 17],
    "SF3": ["calendardate", "Core US Institutional Investors Summary by Ticker", 21],
    "SF3A": ["calendardate", "Core US Institutional Investors Summary by Ticker", 1],
    "SF3B": ["calendardate", "Core US Institutional Investors Summary by Investor", 1],
    "SFP": ["date", "Sharadar Fund Prices", 5],
    "SP500": ["date", "S&P500 Current and Historical Constituents", 1],
}


def db_exists(args):
    # If there is a database designated, check for filename in that datapath.
    if args.save_to == "db":
        file_path = path_save("test", args)
        if not file_path.is_file():
            raise ValueError("No database file matches the directory and name entered.")
        else:
            return None


def get_today():
    """Return today's date."""
    return datetime.date.today().strftime("%Y-%m-%d")


def connect(filepath):
    """
    Creating a connection to a database. If doesn't exist, then warning and create the database.
    :param save_directory: Directory where the database will be housed.
    :param db: Database name.
    :return conn: Returns a connection object.
    """

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


def init_dates(table, args):

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

    assert date_start < date_end

    return date_col, date_start, date_end


def path_directory(args):
    """
    Set path and make directory if necessary
    :param args: Parse args
    :return path: Pathlib path object
    """
    path = Path(args.directory)
    path.mkdir(parents=True, exist_ok=True)

    return path


def path_save(table, args):
    """
    Set csv filepath.
    :param args:
    :return path object:
    """
    path = path_directory(args)
    if args.save_to == "db":
        if args.save_name:
            file_name = args.save_name + ".db"
        else:
            file_name = "data.db"
    elif args.save_to == "csv":
        if args.save_name:
            file_name = args.save_name + "_" + table + ".csv"
        else:
            file_name = table + ".csv"

    return path / file_name


def download_table(table, args):
    """
    Saves the dataframes to csv format:
    :param data: Dataframe downloaded from Quandl.
    :param directory: Directory where csv's will be saved.
    :param table: Table name downloaded and being saved. e.g. 'SEP'
    :return None:
    """

    df_save = pd.DataFrame()
    date_col, date_start, date_end = init_dates(table, args)

    file_path = path_save(table, args)

    if args.save_to == "db":
        # Connect to the database.
        conn = connect(file_path)
        c = conn.cursor()
    else:
        pass

    if args.save_to == "csv" and file_path.is_file():
        # Open csv if exist to dataframe and get maximum date.
        df_save = pd.read_csv(
            file_path, parse_dates=[date_col], infer_datetime_format=True
        ).sort_values(date_col)

        # If there is local data, get the most recent date as start date.
        if df_save.size != 0:
            date_start = (df_save[date_col].max() + pd.Timedelta("1 days")).strftime(
                "%Y-%m-%d"
            )
        else:
            pass

    elif args.save_to == "db" and file_path.is_file():

        # Check if table exist.
        sql_check_table_exist = "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '{}';".format(
            table
        )
        c.execute(sql_check_table_exist)
        if c.rowcount != -1:
            sql = "SELECT MAX({}) FROM {}".format(date_col, table)
            c.execute(sql)
            date_start = (
                pd.to_datetime(c.fetchone()[0]) + pd.Timedelta("1 days")
            ).strftime("%Y-%m-%d")

    else:
        pass

    # Check start and end dates.
    if date_end < date_start:
        raise ValueError(
            "Your start date {} is after your end date {}. This could be caused because the start dates in your "
            "saved data are later than the todate entered.".format(date_start, date_end)
        )

    # Get the data between the dates from Quandl
    kwarg = {date_col: {"gte": date_start, "lte": date_end}}
    df_new = get_data(table, kwarg)

    # Join dataframes if CSV.
    if df_save.size != 0 and args.save_to == "csv":
        df_total = pd.concat([df_save, df_new])
    else:
        df_total = df_new

    if df_total.size != 0:
        if table == "SF3B":
            df_total = df_total.sort_values(date_col, ascending=False)
        else:
            df_total = df_total.sort_values(
                [date_col, "ticker"], ascending=[False, True]
            )

        if args.save_to == "csv":
            df_total.to_csv(file_path, index=False)
        elif args.save_to == "db":
            # Join the new data from Quandl with the existing dataframe and sort, reset index.
            df_new.to_sql(table, con=conn, if_exists="append", index=False)
        else:
            pass

        print(
            "Saved table {} from {} to {}.".format(
                table,
                df_new[date_col].min().strftime("%Y-%m-%d"),
                df_new[date_col].max().strftime("%Y-%m-%d"),
            )
        )
    else:
        print("No data in table {} for saving.".format(table))

    if args.print_rows != 0:
        print(
            "Table: {}, \n{}".format(
                table, df_total.head(pd.to_numeric(args.print_rows))
            )
        )
    else:
        pass

    return df_new


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

    conn = connect(path_save(None, args))
    c = conn.cursor()

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
            elif t == "SF3":
                print(
                    "The SF3 table has very large data sets that exceed the download limits of sharadar so SF3 "
                    "is not downloaded as part of this program. If you wish to download SF3, you must do so "
                    "direclty from the Quandl site using the web api."
                )
    else:
        tables = sharadar_tables.keys()
    print("tables {}".format(tables))

    # Check db exists
    db_exists(args)

    # Open tables, get data, and save.
    for t in tables:
        download_table(t, args)

    c.close()
    conn.close()

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
        "Tables are: SF1 SF2 SF3 EVENTS SF3A SF3B SEP TICKERS DAILY SP500 ACTIONS SFP",
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
    import time
    start_timer = time.time()
    main()

    print("Elapsed time is {:.2f}".format(time.time() - start_timer))
