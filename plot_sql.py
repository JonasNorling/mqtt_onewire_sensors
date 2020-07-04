#!/usr/bin/env python3
#
# Plot readings from a SQL database
#

import argparse
import itertools
import logging
import time
from typing import List, Dict
import sqlite3
import matplotlib.pyplot as plt
import matplotlib as mpl

MAX_FORWARD_FILL = 3600


def get_series_id(db: sqlite3.Connection, name: str) -> int:
    cur = db.execute('SELECT id FROM series WHERE name=?', (name, ))
    result = cur.fetchone()
    if result is not None:
        return result[0]


def plot(db: sqlite3.Connection, series_and_labels: List, start_time: int, end_time: int, step: int):
    times = range(start_time, end_time, step)

    #log.info(mpl.style.available)
    mpl.style.use('dark_background')
    fig, ax = plt.subplots(facecolor='#332222')
    ax.set_facecolor('#332222')
    ax.xaxis.set_major_locator(mpl.dates.HourLocator())
    #ax.xaxis.set_minor_locator(mpl.dates.MinuteLocator())
    ax.xaxis.set_major_formatter(mpl.dates.DateFormatter('%H'))
    #ax.grid(True)

    for series_name, label in series_and_labels:
        series_id = get_series_id(db, series_name)
        if series_id is None:
            log.error(f'No such series {series_name}')
            continue
        cur = db.execute(f'SELECT (time/60*60) as time, value AS "{series_name}" FROM samples '
                         'WHERE series=? AND time >= ? AND time <= ? ORDER BY time',
                         (series_id, start_time - MAX_FORWARD_FILL, end_time + step))
        data = dict(cur)

        list_data = list([None] * len(times))
        last_value = None
        last_time = 0
        for i, t in enumerate(times):
            if t in data:
                list_data[i] = data[t]
                last_value = data[t]
                last_time = t
            elif t <= last_time + MAX_FORWARD_FILL:
                list_data[i] = last_value

        ax.plot(mpl.dates.epoch2num(times), list_data, '.-')

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
    end_time = int(time.time() // 60) * 60
    start_time = end_time - 24 * 60 * 60
    step = 60
    plot(db, series_and_labels, start_time, end_time, step)
