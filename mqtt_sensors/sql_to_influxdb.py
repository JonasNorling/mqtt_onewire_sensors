#!/usr/bin/env python3
#
# Export readings from a SQL database to Influxdb
#

import argparse
import logging
import time
from collections import OrderedDict
import datetime
from typing import List, Dict, Tuple
import sqlite3

import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS

def get_series_id(db: sqlite3.Connection, name: str) -> int:
    cur = db.execute('SELECT id FROM series WHERE name=?', (name, ))
    result = cur.fetchone()
    if result is not None:
        return result[0]


def export_series(db: sqlite3.Connection, write_api: influxdb_client.WriteApi, bucket: str, org: str, series_name: str, start_time: int, end_time: int):
    series_id = get_series_id(db, series_name)
    if series_id is None:
        log.error(f'No such series {series_name}')
        return
    cur = db.execute(f'SELECT (time/60*60) as time, value AS "{series_name}" FROM samples '
                     'WHERE series=? AND time >= ? AND time <= ? ORDER BY time',
                     (series_id, start_time, end_time))
    data: List[Tuple[int, float]] = list(cur)
    for timestamp, value in data:
        point = Point('sample') \
            .field(series_name, float(value)) \
            .tag('series', series_name) \
            .time(timestamp, WritePrecision.S)
        write_api.write(bucket, org, point)
    log.info(f'Wrote {len(data)} samples')


def export(args):
    db = sqlite3.connect(f'file:{args.db}?mode=ro', uri=True)
    end_time = int(time.time() // 60) * 60
    start_time = end_time - args.time

    with InfluxDBClient(args.url, token=args.token, org=args.org) as influx:
        with influx.write_api() as write_api:
            for series_name in args.series:
                log.info(f'Exporting series {series_name}')
                export_series(db, write_api, args.bucket, args.org, series_name, start_time, end_time)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('plot_sql')

    parser = argparse.ArgumentParser(description="Plot readings from a SQL database")
    parser.add_argument(nargs="+", dest="series", metavar="SERIES", help="Data series")
    parser.add_argument("--db", metavar="FILE", default="samples.sqlite", help="SQLite database file")
    parser.add_argument("--time", metavar="SECONDS", type=int, default=24*60*60, help="Time span to export, in seconds")
    parser.add_argument("--url", metavar="URL", default="http://localhost:8086", help="Influxdb URL")
    parser.add_argument("--bucket", metavar="BUCKET",  help="Bucket name", required=True)
    parser.add_argument("--org", metavar="ORG",  help="Organization name", required=True)
    parser.add_argument("--token", metavar="TOKEN",  help="Influxdb access token", required=True)
    parser.add_argument("--debug", default=False, action="store_true", help="Enable debug printouts")
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel('DEBUG')

    export(args)
