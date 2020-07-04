#!/usr/bin/env python3
#
# Plot readings from a SQL database
#

import argparse
import itertools
import logging
import time
from collections import OrderedDict
from typing import List, Dict, Tuple
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
    #log.info(mpl.style.available)
    mpl.style.use('dark_background')
    fig, ax = plt.subplots(facecolor='#332222', figsize=(10, 4))
    ax.set_facecolor('#332222')
    ax.xaxis.set_major_locator(mpl.dates.HourLocator())
    ax.xaxis.set_major_formatter(mpl.dates.DateFormatter('%H'))
    ax.set_xlim(mpl.dates.epoch2num(start_time), mpl.dates.epoch2num(end_time))
    ax.grid(color='#444444')

    for series_name, label in series_and_labels:
        series_id = get_series_id(db, series_name)
        if series_id is None:
            log.error(f'No such series {series_name}')
            continue
        cur = db.execute(f'SELECT (time/60*60) as time, value AS "{series_name}" FROM samples '
                         'WHERE series=? AND time >= ? AND time <= ? ORDER BY time',
                         (series_id, start_time - MAX_FORWARD_FILL, end_time + MAX_FORWARD_FILL))
        data: List[Tuple[int, float]] = list(cur)
        last_time = None
        # Insert dummy samples to inhibit lines that are too long
        for i, (t, v) in enumerate(data):
            if last_time is not None and t > last_time + MAX_FORWARD_FILL:
                data.insert(i, (t-1, None))
            last_time = t
        ax.plot(mpl.dates.epoch2num([t for t, v in data]), [v for t, v in data],
                '-', label=label)

    ax.legend()
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
