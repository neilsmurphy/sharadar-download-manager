from update import sharadar_tables, get_today, init_dates, path_directory, path_csv
import datetime

# Class for including args.
class ArgsTest:
    def __init__(self, todate=None, fromdate=None, directory=None, save_name=None):
        self.todate = todate
        self.fromdate = fromdate
        self.directory = directory
        self.save_name = save_name


def test_today():
    result = get_today()
    assert result == datetime.date.today().strftime("%Y-%m-%d")


def test_sharadar_tables():
    tables = [
        "SF1",
        "SF2",
        "SF3",
        "EVENTS",
        "SF3A",
        "SF3B",
        "SEP",
        "TICKERS",
        "DAILY",
        "SP500",
        "ACTIONS",
        "SFP",
    ]
    for t in tables:
        assert t in sharadar_tables


def test_init_dates_col_date():
    at = ArgsTest()
    tables = ["ACTIONS", "SEP", "ACTIONS", "EVENTS", "SFP", "SP500"]
    for t in tables:
        date_col, date_start, date_end = init_dates(t, at)
        assert date_col == "date"


def test_init_dates_col_lastupdated():
    at = ArgsTest()
    tables = ["SF1", "DAILY", "TICKERS"]
    for t in tables:
        date_col, date_start, date_end = init_dates(t, at)
        assert date_col == "lastupdated"


def test_init_dates_calendardate():
    at = ArgsTest()
    tables = ["SF3", "SF3A", "SF3B"]
    for t in tables:
        date_col, date_start, date_end = init_dates(t, at)
        assert date_col == "calendardate"


def test_init_dates_filingdate():
    at = ArgsTest()
    tables = ["SF2"]
    for t in tables:
        date_col, date_start, date_end = init_dates(t, at)
        assert date_col == "filingdate"


def test_init_dates_nostart_noend():
    at = ArgsTest()
    tables = "ACTIONS"
    _, date_start, date_end = init_dates(tables, at)
    assert date_start == "2000-01-01"
    assert date_end == datetime.date.today().strftime("%Y-%m-%d")


def test_init_dates_setstart_fixend():
    at = ArgsTest(fromdate="2020-01-01", todate="2020-02-01")

    tables = "ACTIONS"
    _, date_start, date_end = init_dates(tables, at)
    assert date_start == "2020-01-01"
    assert date_end == "2020-02-01"


def test_path_directory():
    dir = "testing"
    at = ArgsTest(directory=dir)
    assert path_directory(at).name == dir


def test_path_csv():
    table = "EVENTS"
    save_name = "mytest"
    dir = "data"
    at = ArgsTest(directory=dir, save_name=save_name)

    assert str(path_csv(table, at)) == "{}/{}_{}.csv".format(dir, save_name, table)
