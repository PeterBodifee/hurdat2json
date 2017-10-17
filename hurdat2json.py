#!/usr/bin/env python

# Author: Peter Bodifee
# Date: 2017-10-17

# This utility converts public NOAA huricane data to JSON format
# for easier ingestion into analysis tools
# http://www.nhc.noaa.gov/data/hurdat/hurdat2-format-atlantic.pdf

VERSION='0.2'

# standard library imports
import sys
import argparse
import signal
import json 
import datetime
import aniso8601

# signal handler
def signal_handler(signal, frame):
    exit(0)

# command line parser
def get_cli_parser():

    # set up the argument parser
    parser = argparse.ArgumentParser(
        description='Convert NOAA Huricane track data to JSON')
    
    # version option
    parser.add_argument('--version', 
                        action='version', 
                        version=VERSION
                        )
    # debug option
    parser.add_argument("--debug",
                        action="store_true",
                        dest="Debug",
                        default=False,
                        help=argparse.SUPPRESS
                       )

    # input file parameter (optional, otherwise std input)
    parser.add_argument("input_file",
                        help='Input file is the Huricane Data'
                       )
    
    return parser


def convert_position(s):
    # in the huricane data position is formatted like 29.3N and 70.2W
    # to make the position numeric there is a sign
    # S an W are negative, N and E are positive
    if s[-1:] in ['S','W']:
        return -1 * float(s[:-1])
    elif s[-1:] in ['N','E']:
        return float (s[:-1])
    else:
        return None

def convert_date_time_iso8601(d, t):
    # in the huricane data date and time are 2 seperate fields,
    # formatted <YYYYMMDD> and <HHMM>
    # this function returns a string in ISO8601 format:
    # <YYYY>-<MM>-<DD>T<HH>:<MM>:<SS>
    dt = datetime.datetime.combine( aniso8601.parse_date(d),
                                    aniso8601.parse_time(t))
    return dt.isoformat()

def get_huricane_data(fields):
    # put header in huricane dict
    huricane = {}
    # basin = 1st field pos 1-2
    huricane['basin'] = fields[0][0:2]    
    # cyclone number = 1st field pos 3-4
    huricane['cyclone_nr'] = int(fields[0][2:4])    
    # year = 1st field pos 5-8
    huricane['year'] = int(fields[0][4:8])    
    # name = 2nd field
    huricane['name'] = fields[1]
    # nr of tracks = 3rd field
    huricane['nr_of_tracks'] = int(fields[2])

    return huricane

def get_track_data(fields):
    # process track data
    track = {}
    # combine date and time fields
    track["date_time"] = convert_date_time_iso8601(fields[0], fields[1])
    track["identifier"] = fields[2]
    track["status"] = fields[3]
    # latitude and longitude need to be signed
    track["latitude"] = convert_position(fields[4])
    track["longitude"] = convert_position(fields[5])
    # wind speed and pressure are numbers
    track["max_wind_speed"] = int(fields[6])
    track["min_pressure"] = int(fields[7])
    # process wind radii in track record (4 quadrants per 3 wind radii)
    wind_radii_keys = ['34_kt_wind_radii', 
                       '50_kt_wind_radii', 
                       '68_kt_wind_radii' ]
    quadrant_keys = ["NE","SE","SW","NW"]
    for wr in range(len(wind_radii_keys)):
        wind_radii = {}
        for qd in range(len(quadrant_keys)):
            field_nr = 8 + (wr*4) + qd
            wind_radii[quadrant_keys[qd]] = int(fields[field_nr])
        track[wind_radii_keys[wr]] = wind_radii

    return track


def main():

    # catch OS interrupt signal for a normal termination
    signal.signal(signal.SIGINT, signal_handler)
    # catch OS broken pipe for a normal termination
    signal.signal(signal.SIGPIPE, signal_handler)
    
    # process command line arguments.
    args = get_cli_parser().parse_args()

    # open input 
    try:
        fp = open(args.input_file, 'r')

    except Exception as e:
        print(str(e), file=sys.stderr)

    else:
        # read first line
        line = fp.readline()
        # do until all lines have been read
        while line:
            # fields are comma seperated, remove blanks
            fields = line.replace(' ','').split(',')

            if args.Debug:
                print ("DEBUG fields:", fields)

            # line with 4 fields is the huricane header record 
            # line with 21 fields is track record 

            if len(fields) == 4:
                # create a new dict instance for a huricane
                huricane = get_huricane_data(fields)

                if args.Debug:
                    print ("DEBUG huricane header:", huricane)

                # initialize track counter and tracks list
                track_cnt = 0
                tracks = []

            elif len(fields) == 21:
                # increment track counter
                track_cnt += 1

                # create new dict for track data
                track = get_track_data(fields)

                if args.Debug:
                    print ("DEBUG track cnt:", track_cnt)
                    print ("DEBUG track:", track)

                # add track to the tracks list
                tracks.append(track)

                # when at last track add tracks to huricane and 
                # output huricane data in JSON
                if track_cnt == huricane['nr_of_tracks']:
                    huricane['tracks']=tracks
                    print(json.dumps(huricane))

            line = fp.readline()        # next line 

    finally:
        fp.close()

    return

if __name__ == "__main__":
    main()


