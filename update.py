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
import argparse
import datetime
from pathlib import Path
import sqlite3
import time

import pandas as pd
import quandl

from config import apikey

"""
For downloading zip files. 
https://www.quandl.com/tables/SHARADAR-SF1/export?api_key=insert_key
"""
sharadar_tables = {
    "TICKERS": ["lastupdated", "Tickers and Metadata", 1],
    "EVENTS": ["date", "Core US Fundamental Events", 1],
    "SF3A": ["calendardate", "Core US Institutional Investors Summary by Ticker", 1],
    "SF3B": ["calendardate", "Core US Institutional Investors Summary by Investor", 1],
    "ACTIONS": ["date", "Corporate Actions", 1],
    "SP500": ["date", "S&P500 Current and Historical Constituents", 1],
    "SFP": ["date", "Sharadar Fund Prices", 5],
    "SF1": ["lastupdated", "Core US Fndamentals", 7],
    "DAILY": ["lastupdated", "Daily Metrics", 9],
    "SEP": ["date", "Sharadar Equity Prices", 11],
    "SF2": ["filingdate", "Core US Insiders", 17],
    "SF3": ["calendardate", "Core US Institutional Investors Summary by Ticker", 21],
}


def db_exists(args):
    # If there is a database designated, check for filename in that datapath.
    file_path = path_save(args)
    if not file_path.is_file():
        raise ValueError("No database file matches the directory and name entered.")
    else:
        return None


def set_tables(args):
    # Set the tables.
    if args.tables:
        tables = args.tables
        for t in tables:
            if t not in sharadar_tables:
                raise ValueError(
                    "The table {} is not a valid table. Valid table names are: {}".format(
                        t, sharadar_tables.keys()
                    )
                )
    else:
        tables = sharadar_tables.keys()

    if args.print_on:
        print("tables {}".format(tables))

    return tables


def get_all_tickers(args):
    # Get a list of all the tickers in the db.
    sql = "SELECT DISTINCT ticker FROM tickers"
    conn = connect(path_save(args))
    all_tickers = pd.read_sql(sql, con=conn)
    conn.close()
    all_tickers = sorted(all_tickers["ticker"].to_list())

    return all_tickers


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
        date_end = pd.to_datetime(get_today()) - pd.Timedelta("1 days")
        date_end = date_end.strftime("%Y-%m-%d")

    return date_col, date_end


def path_directory(args):
    """
    Set path and make directory if necessary
    :param args: Parse args
    :return path: Pathlib path object
    """
    path = Path(args.directory)
    path.mkdir(parents=True, exist_ok=True)

    return path


def path_save(args):
    """
    Set filepath.
    :param args:
    :return path object:
    """
    path = path_directory(args)
    if args.save_name:
        file_name = args.save_name + ".db"
    else:
        file_name = "data.db"

    return path / file_name


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def accumulate_results(df_downloaded, t, args):
    """ Collect the downloaded tables in a `side` table."""

    # Validate accumulated table and append to original.
    table_name = t + "_cumm"
    conn = connect(path_save(args))
    df_downloaded.to_sql(table_name, con=conn, if_exists="append", index=False)
    conn.close()


def consolidate_results(t, tstart, args):
    # Validate accumulated table and append to original.
    table_name = t + "_cumm"
    sql = "SELECT * FROM " + table_name

    conn = connect(path_save(args))
    try:
        df_cumm = pd.read_sql(sql, con=conn)
    except:
        if args.print_on:
            print(
                "There was no downloaded data to add from {} to {}.".format(
                    table_name, t
                )
            )
        conn.close()
        return

    if df_cumm.size > 0:
        df_cumm.to_sql(t, con=conn, if_exists="append", index=False)
        if args.print_on:
            print("Completed download of {} in {}".format(t, time.time() - tstart))
    else:
        if args.print_on:
            print("No data to add to {}".format(t))

    c = conn.cursor()
    sql = "DROP TABLE {}".format(table_name)
    c.execute(sql)
    conn.close()


def download_table(table, args, tc=None):
    """
    Saves Quandl data to local sqlite3 database.
    :param table: Table to be downloaded from Quandl.
    :param args: See --help in args.parse
    :return None:
    """

    date_col, date_end = init_dates(table, args)

    # Get filepath.
    file_path = path_save(args)

    # Check if table exist and get the start date. If table doesn't exist, set start date to one week before end date.
    sql_check_table_exist = "SELECT name FROM sqlite_master WHERE type = 'table' AND tbl_name = '{}';".format(
        table
    )
    conn = connect(file_path)
    c = conn.cursor()
    c.execute(sql_check_table_exist)

    if len(c.fetchall()) == 1:
        sql = "SELECT MAX({}) FROM {}".format(date_col, table)
        c.execute(sql)
        date_start = (
            pd.to_datetime(c.fetchone()[0]) + pd.Timedelta("1 days")
        ).strftime("%Y-%m-%d")
    else:
        date_start = pd.to_datetime(date_end) - pd.Timedelta("7 days")
        date_start = date_start.strftime("%Y-%m-%d")
        if args.print_on:
            print(date_start)

    conn.close()

    # Get the data between the dates from Quandl and restricted for tickers if tc not null.
    if tc and len(tc) != 0:
        kwarg = {date_col: {"gte": date_start, "lte": date_end}, "ticker": list(tc)}
    else:
        kwarg = {date_col: {"gte": date_start, "lte": date_end}}

    try:
        df_new = get_data(table, kwarg)
    except:
        return 1

    if df_new.size == 0:
        if tc:
            if args.print_on:
                print(
                    "No data retrieved for table {} from {} to {}".format(
                        table, tc[0], tc[-1]
                    )
                )
        else:
            if args.print_on:
                print("No data retrieved for table {}".format(table))
        return df_new

    if table == "SF3B":
        df_new = df_new.sort_values(date_col, ascending=False)
    else:
        df_new = df_new.sort_values([date_col, "ticker"], ascending=[False, True])

    # Join the new data from Quandl with the existing dataframe and sort, reset index.
    table_name = table + "_cumm"
    conn = connect(file_path)
    df_new.to_sql(table_name, con=conn, if_exists="append", index=False)
    conn.close()

    if args.print_on:
        print(
            "Saved table {} date from {} to {}, ticker from {} to {}.".format(
                table,
                df_new[date_col].min().strftime("%Y-%m-%d"),
                df_new[date_col].max().strftime("%Y-%m-%d"),
                df_new["ticker"].min(),
                df_new["ticker"].max(),
            )
        )

    if args.print_rows != 0 and df_new.shape[0] != 0:
        if args.print_on:
            print(
                "Table: {}, shape {} \n{}".format(
                    table, df_new.shape, df_new.head(pd.to_numeric(args.print_rows))
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
    start_timer = time.time()

    # api_key = args.key    ### todo USE THIS LINE IN LIVE VERSION.
    api_key = apikey  ### todo delete this
    quandl.ApiConfig.api_key = api_key

    tables = set_tables(args)

    # Check db exists
    db_exists(args)

    tstart = time.time()

    for t in tables:
        # Check if table is a large table (>1), if so, break into chunks by ticker.
        if sharadar_tables[t][2] > 1:
            all_tickers = get_all_tickers(args)

            # Get tickers in chunks of 1000.
            ticker_chunks = chunks(all_tickers, 1000)
            for tc in ticker_chunks:
                if args.print_on:
                    print(
                        "\nDownloading stocks in {} from {} to {}".format(
                            t, tc[0], tc[-1]
                        )
                    )
                df_downloaded = download_table(t, args, tc=tc)
                if isinstance(df_downloaded, (int, float)) and df_downloaded==1:
                    if args.print_on:
                        print(
                            "There was an error connecting to quandl for stocks in {} from {} to {}".format(
                                t, tc[0], tc[-1]
                            )
                        )
                    continue
                elif df_downloaded.size != 0:
                    accumulate_results(df_downloaded, t, args)
        else:
            # For smaller tables, download the whole table at once.
            df_downloaded = download_table(t, args)
            if isinstance(df_downloaded, (int, float)) and df_downloaded==1:
                if args.print_on:
                    print("There was an error connecting to quandl for stocks in {}".format(t))
                continue
            elif df_downloaded.size != 0:
                accumulate_results(df_downloaded, t, args)

        consolidate_results(t, tstart, args)

        tstart = time.time()

    if args.print_on:
        print("Elapsed time is {:.2f}".format(time.time() - start_timer))

    return None


def add_bool_arg(parser, name, default=False, help_text=""):
    """ Sets the boolean args.parse values. """
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
        "--todate",
        required=False,
        default="",
        help="Date to download data to. If blank, download to today's date.",
    )

    parser.add_argument(
        "--save_name",
        required=False,
        default="sharadar",
        help="If provided will be the name of the database, or the pre-pend text to the csv. \n"
        "For example, if 'mydata' and csv table is DAILY, then file will be 'mydata_DAILY.csv'",
    )

    parser.add_argument(
        "--directory",
        required=False,
        default="data",
        help="Sub directory to save the csv or database files in.",
    )

    add_bool_arg(
        parser, "print_on", False, "Print output. --print_on True; --no-print_on False"
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
# todo make print messages log as well to file.
