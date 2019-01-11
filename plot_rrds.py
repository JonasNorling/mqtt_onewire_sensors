#!/usr/bin/env python3
#
# Plot temperature readings from RRDs
#

import argparse
import logging
import subprocess

COLORS = [ 0x4488ee, 0xee4488, 0x88ee44, 0xbb6622,
    0x6622bb, 0x22bb66, 0x2222ee, 0x22ee22, 0xee2222 ]
BG_COLOR = 0x332222
FG_COLOR = 0xffffff

SIZE = (768, 256)
COMMON_OPTS = "-E --lazy --full-size-mode --grid-dash 1:0"
COLOR_OPTS = "--color SHADEA#{BG:06x} --color SHADEB#{BG:06x} \
--color BACK#{BG:06x} --color CANVAS#{BG:06x} \
--color FONT#{FG:06x} --color AXIS#{FG:06x} --color ARROW#{FG:06x} \
--color GRID#444444 --color MGRID#777777".format(
    BG=BG_COLOR, FG=FG_COLOR)

def plot(rrd_files, outdir, timespan="1d"):
    log.debug("Plotting %s" % timespan)
    start = "now-%s" % timespan
    filename = "%s/plot-%s.png" % (outdir, timespan)
    defs = ""
    elements = ""
    for i, rrd in enumerate(rrd_files):
        defs += ("DEF:sensor{i}={rrd}:value:AVERAGE " +
                 "DEF:sensor{i}_min={rrd}:value:MIN " +
                 "DEF:sensor{i}_max={rrd}:value:MAX " +
                 "CDEF:sensor{i}_delta=sensor{i}_max,sensor{i}_min,- "
                ).format(i = i, rrd = rrd)
        elements += ("AREA:sensor{i}_min#{color:06x}1e " +
                     "LINE2:sensor{i}_min#{color:06x}:'{label}' " +
                     "AREA:sensor{i}_delta#{color:06x}::STACK "
                ).format(i = i, color = COLORS[i], label = "thing")
    try:
        completed = subprocess.run(["rrdtool", "graph",
            filename,
            *COMMON_OPTS.split(),
            *COLOR_OPTS.split(),
            "--end", "now", "--start", start,
            "--width", str(SIZE[0]), "--height", str(SIZE[1]),
            *defs.split(), *elements.split()],
            stdout=subprocess.PIPE)
        if completed.returncode != 0:
            log.error("RRD plot failed")
            log.debug(completed)
    except FileNotFoundError as e:
        log.error(e)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("mqtt_1w")

    parser = argparse.ArgumentParser(description="Publish one-wire sensor values with MQTT")
    parser.add_argument(nargs="+", dest="rrd", metavar="RRD", help="RRD files")
    parser.add_argument("--debug", default=False, action="store_true",
                        help="Enable debug printouts")
    parser.add_argument("--outdir", default=".", metavar="DIR", help="Output directory")
    parser.add_argument("-t", action="append",
                        help="Timespan to plot (can be repeated)")
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel('DEBUG')

    timespans = ["1d", "1w", "1m", "1y"]
    if args.t is not None:
        timespans = args.t
    for timespan in timespans:
        plot(args.rrd, args.outdir, timespan)
