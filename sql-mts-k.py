#!/bin/python3

import sys
import time
import toml
import dacite
import typing
import sqlite3
import pathlib
import requests
import datetime
import schedule
import dataclasses


def info(message):
    if sys.stdout.isatty():
        print(f"\033[1m[\033[32minfo\033[0;1m]\033[0m {message}",
              file=sys.stderr)
    else:
        print(f"[info] {message}", file=sys.stderr)


def error_message(message):
    if sys.stdout.isatty():
        print(f"\033[1m[\033[31merror\033[0;1m]\033[0m {message}",
              file=sys.stderr)
    else:
        print(f"[error] {message}", file=sys.stderr)


def error(message):
    error_message(message)
    exit(1)


def sqlite3_insert_dataclass(cursor: sqlite3.Cursor,
                             table_name: str,
                             dataclass: typing.Any,
                             or_ignore: bool = False):
    if not dataclasses.is_dataclass(dataclass):
        raise ValueError()

    fields = dataclasses.fields(dataclass)
    row = tuple([getattr(dataclass, field.name) for field in fields])
    columns = ','.join([field.name for field in fields])
    params = ','.join(['?'] * len(fields))

    string = f"INSERT {'OR IGNORE' if or_ignore else ''} \
               INTO {table_name} ({columns}) VALUES ({params});"
    cursor.execute(string, row)


def sqlite3_create_table_for_dataclass(cursor: sqlite3.Cursor,
                                       table_name: str,
                                       dataclass: typing.Any,
                                       primary_key: None | str | tuple[str, ...] = None,
                                       if_not_exists: bool = False):
    if not dataclasses.is_dataclass(dataclass):
        raise ValueError()

    fields = dataclasses.fields(dataclass)
    field_names = [field.name for field in fields]

    if isinstance(primary_key, str):
        if at := field_names.index(primary_key) < 0:
            raise ValueError()

        field_names[at] += " PRIMARY KEY"

    if isinstance(primary_key, tuple):
        field_names.append(f"PRIMARY KEY ({','.join(primary_key)})")

    columns = ','.join(field_names)

    string = f"CREATE TABLE {'IF NOT EXISTS' if if_not_exists else ''} \
               {table_name} ({columns});"
    cursor.execute(string)


@dataclasses.dataclass
class SqlStatus():
    stationId: str
    isOpen: bool


@dataclasses.dataclass
class SqlMtsKConfig():
    tries: int
    timeout: int
    interval: int
    database_path: str


@dataclasses.dataclass
class TankerkoenigConfig():
    apikey: str
    type: str
    lat: float
    lng: float
    rad: int
    sort: str


@dataclasses.dataclass
class Config():
    sql_mts_k: typing.Optional[SqlMtsKConfig]
    tankerkoenig: TankerkoenigConfig

    @staticmethod
    def from_file(filepath: pathlib.Path | str) -> 'Config':
        data = toml.loads(open(filepath).read())
        return dacite.from_dict(data_class=Config, data=data)


@dataclasses.dataclass
class SqlStation():
    id: str
    name: str
    brand: str
    street: str
    place: str
    lat: float
    lng: float
    dist: float
    houseNumber: str
    postCode: int


@dataclasses.dataclass
class SqlPrice():
    stationId: str
    timestamp: int
    price: float


@dataclasses.dataclass
class TankerkoenigStation():
    id: str
    name: str
    brand: str
    street: str
    place: str
    lat: float
    lng: float
    dist: float
    price: float | None
    isOpen: bool
    houseNumber: str
    postCode: int


@dataclasses.dataclass
class TankerkoenigListResponse():
    ok: bool
    status: str
    message: typing.Optional[str]
    stations: typing.Optional[list[TankerkoenigStation]]

    def insert_into_database(self,
                             cursor: sqlite3.Cursor,
                             timestamp: int | None = None):
        assert self.stations is not None

        if timestamp is None:
            now = datetime.datetime.now()
            timestamp = round(datetime.datetime.timestamp(now))

        for station in self.stations:
            sql_station = SqlStation(station.id,
                                     station.name,
                                     station.brand,
                                     station.street,
                                     station.place,
                                     station.lat,
                                     station.lng,
                                     station.dist,
                                     station.houseNumber,
                                     station.postCode)

            if station.price is None:
                continue

            sql_status = SqlStatus(station.id, station.isOpen)
            sql_price = SqlPrice(station.id, timestamp, station.price)

            sqlite3_insert_dataclass(cursor,
                                     'stations',
                                     sql_station,
                                     or_ignore=True)

            sqlite3_insert_dataclass(cursor,
                                     'status',
                                     sql_status,
                                     or_ignore=True)

            sqlite3_insert_dataclass(cursor,
                                     'prices',
                                     sql_price,
                                     or_ignore=True)


CONFIG_PATH = "/etc/sql-mts-k.toml"

tankerkoenig_params: dict[str, typing.Any]
sql_mts_k = SqlMtsKConfig(tries=3, timeout=10, interval=360,
                          database_path="/etc/sql_mts_k.db")
con: sqlite3.Connection
cur: sqlite3.Cursor


def fetch_current_prices():
    global sql_mts_k, tankernoenig_params, con, cur

    url = "https://creativecommons.tankerkoenig.de/json/list.php"

    for i in range(0, sql_mts_k.tries):
        response = requests.get(url=url, params=tankerkoenig_params)
        if response.status_code == 200:
            break

        error_message(f"Retrying {i+1}/{sql_mts_k.tries}")
        time.sleep(sql_mts_k.timeout)
    else:
        error_message(f"Unable to fetch after {sql_mts_k.tries} tries")
        return

    data = dacite.from_dict(data_class=TankerkoenigListResponse,
                            data=response.json())

    if not data.ok:
        error_message(data.message)

    data.insert_into_database(cur)
    con.commit()

    info("Successfully fetched the current prices")


try:
    config = Config.from_file(CONFIG_PATH)

    if config.sql_mts_k is not None:
        sql_mts_k = config.sql_mts_k

    tankerkoenig_params = dataclasses.asdict(config.tankerkoenig)

    # "Home-Automation-, Smart-Mirror- und ähnliche Systeme sollten Abfragen
    # nicht öfter als einmal in 5 Minuten durchführen"
    #
    # If every retry fails, it takes roughly (TRIES * RETRY_TIMEOUT) seconds.
    # Since the task took a long time to complete, schedule will then
    # reschedule it earlier.
    if sql_mts_k.interval < (5*60) + (sql_mts_k.tries * sql_mts_k.timeout):
        raise Exception("`interval-tries*timeout` must be greater than 5 min")

except Exception as e:
    error(f"Unable to load config: {str(e)}")

con = sqlite3.connect(sql_mts_k.database_path)
cur = con.cursor()

sqlite3_create_table_for_dataclass(cur,
                                   'stations',
                                   SqlStation,
                                   primary_key='id',
                                   if_not_exists=True)
sqlite3_create_table_for_dataclass(cur,
                                   'prices',
                                   SqlPrice,
                                   primary_key=('stationId', 'timestamp'),
                                   if_not_exists=True)
sqlite3_create_table_for_dataclass(cur,
                                   'status',
                                   SqlStatus,
                                   primary_key=('stationId'),
                                   if_not_exists=True)

schedule.every(sql_mts_k.interval).seconds.do(fetch_current_prices)

info("Initialization successful")

while True:
    seconds_until_next_run = schedule.idle_seconds()
    assert seconds_until_next_run is not None

    time.sleep(seconds_until_next_run)
    schedule.run_pending()
