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


CONFIG_LOCATION = "/etc/sql-mts-k.toml"
DATABASE_LOCATION = "/etc/sql-mts-k.db"

# "Home-Automation-, Smart-Mirror- und ähnliche Systeme sollten Abfragen nicht öfter als einmal in 5 Minuten durchführen"
# ==> SCHEDULE > 5 min + (TRIES * RETRY_TIMEOUT)
SCHEDULE = schedule.every(6).minutes
TRIES = 3
RETRY_TIMEOUT = 10


config: 'Config'
con: sqlite3.Connection
cur: sqlite3.Cursor


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
class Config():
    apikey: str
    type: str
    lat: float
    lng: float
    rad: int
    sort: str

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


def fetch_current_prices():
    global config, con, cur

    url = "https://creativecommons.tankerkoenig.de/json/list.php"
    params = dataclasses.asdict(config)

    for i in range(0, TRIES):
        response = requests.get(url=url, params=params)
        if response.status_code == 200:
            break

        error_message(f"Retrying {i+1}/{TRIES}")
        time.sleep(RETRY_TIMEOUT)
    else:
        error_message(f"Unable to fetch data after {TRIES} tries")
        return

    data = dacite.from_dict(data_class=TankerkoenigListResponse,
                            data=response.json())

    if not data.ok:
        error_message(data.message)

    data.insert_into_database(cur)
    con.commit()

    info("Successfully fetched the current prices")


try:
    config = Config.from_file(CONFIG_LOCATION)
except Exception as e:
    error(f"Unable to load config: {str(e)}")

con = sqlite3.connect(DATABASE_LOCATION)
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

SCHEDULE.do(fetch_current_prices)

while True:
    seconds_until_next_run = schedule.idle_seconds()
    assert seconds_until_next_run is not None

    time.sleep(seconds_until_next_run)
    schedule.run_pending()
