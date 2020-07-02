#!/usr/bin/env python3
#
# Plot readings from a SQL database
#

import argparse
import itertools
import logging
from typing import List
import pandas
import sqlite3
import matplotlib.pyplot as plt
import matplotlib as mpl


def get_series_id(db: sqlite3.Connection, name: str) -> int:
    cur = db.execute('SELECT id FROM series WHERE name=?', (name, ))
    result = cur.fetchone()
    if result is not None:
        return result[0]


def plot(db: sqlite3.Connection, series_and_labels: List):
    result = []
    for series_name, label in series_and_labels:
        series_id = get_series_id(db, series_name)
        if series_id is None:
            log.error(f'No such series {series_name}')
            continue
        df = pandas.read_sql_query(f'SELECT time, value AS "{series_name}" FROM samples WHERE series=?',
                                   db,
                                   index_col='time',
                                   parse_dates=['time'],
                                   params=(series_id,))
        result.append(df)
    data: pandas.DataFrame = pandas.concat(result, sort=True)

    log.info(mpl.style.available)
    mpl.style.use('dark_background')
    fig, ax = plt.subplots(facecolor='#332222')
    ax.set_facecolor('#332222')
    ax.plot(data, '.-')
    plt.savefig('plot.png')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('plot_sql')

    parser = argparse.ArgumentParser(description="Plot readings from a SQL database")
    parser.add_argument(nargs="+", dest="series", metavar="SERIES", help="Data series")
    parser.add_argument("-l", action="append", help="Data label (can be repeated)")
    parser.add_argument("--db", metavar="FILE", default="samples.sqlite", help="SQLite database file")
    parser.add_argument("--debug", default=False, action="store_true",
                        help="Enable debug printouts")
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel('DEBUG')

    if args.l is None:
        args.l = []
    series_and_labels = list(itertools.zip_longest(args.series, args.l, fillvalue="sensor"))

    db = sqlite3.connect(f'file:{args.db}?mode=ro', uri=True)
    plot(db, series_and_labels)
