#!/usr/bin/env python3
#
# Plot readings from a SQL database
#

import argparse
import itertools
import logging
import time
import datetime
from typing import List, Tuple
import sqlite3
import matplotlib.pyplot as plt
import matplotlib as mpl

MAX_FORWARD_FILL = 3600
BG_COLOR = '#332222'

log = logging.getLogger('plot_sql')


def get_series_id(db: sqlite3.Connection, name: str) -> int:
    cur = db.execute('SELECT id FROM series WHERE name=?', (name, ))
    result = cur.fetchone()
    if result is not None:
        return result[0]


def plot_series(db: sqlite3.Connection, ax, series_name: str, label: str, start_time: int, end_time: int):
    series_id = get_series_id(db, series_name)
    if series_id is None:
        log.error(f'No such series {series_name}')
        return
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
    ax.plot([datetime.datetime.utcfromtimestamp(t) for t, v in data],
            [v for t, v in data],
            '-', label=label)


def plot(args):
    series_and_labels = list(itertools.zip_longest(args.series, args.l, fillvalue="sensor"))

    db = sqlite3.connect(f'file:{args.db}?mode=ro', uri=True)
    end_time = int(time.time() // 60) * 60
    start_time = end_time - args.time

    hour_count = (end_time - start_time) // (60 * 60)
    day_count = hour_count // 24

    mpl.style.use('dark_background')
    fig, ax = plt.subplots(facecolor=BG_COLOR, figsize=(9, 3))
    ax.set_facecolor(BG_COLOR)
    tz = datetime.datetime.now().astimezone().tzinfo
    ax.xaxis_date(tz=tz)
    if hour_count < 48:
        ax.xaxis.set_major_locator(mpl.dates.HourLocator(tz=tz))
        ax.xaxis.set_major_formatter(mpl.dates.DateFormatter('%H', tz=tz))
    ax.set_xlim(datetime.datetime.utcfromtimestamp(start_time),
                datetime.datetime.utcfromtimestamp(end_time))
    ax.grid(color='#444444')

    for series_name, label in series_and_labels:
        plot_series(db, ax, series_name, label, start_time, end_time)

    fig.legend(loc='upper center', ncol=len(series_and_labels), frameon=False)
    plt.savefig(args.out, facecolor=BG_COLOR)


def run():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Plot readings from a SQL database")
    parser.add_argument(nargs="+", dest="series", metavar="SERIES", help="Data series")
    parser.add_argument("-l", metavar="LABEL", action="append", help="Data label (can be repeated)")
    parser.add_argument("--db", metavar="FILE", default="samples.sqlite", help="SQLite database file")
    parser.add_argument("--time", metavar="SECONDS", type=int, default=24*60*60, help="Time span to plot, in seconds")
    parser.add_argument("--out", metavar="FILENAME", default="plot.png", help="Output filename")
    parser.add_argument("--debug", default=False, action="store_true", help="Enable debug printouts")
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel('DEBUG')

    if args.l is None:
        args.l = []

    plot(args)


if __name__ == '__main__':
    run()
