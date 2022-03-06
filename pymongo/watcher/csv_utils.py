#!/usr/bin/env python3

import argparse
import csv
import sys
import shutil
import tempfile


def optional_to_int(n):
    """
    Converts n to int if possible and returns n itself in case of
    exception.
    """
    try:
        return int(n)
    except Exception:
        return n


def aggregate(args):
    """
    aggregate sub-command
    """
    if args.in_place and sys.stdin in args.infiles:
        print("Cannot use --in-place with standard input.", file=sys.stderr)
        exit(1)

    # A mapping from WatchID to latest available iteration number. If
    # args.iteration_column=-1 and we had to use the latest line, we
    # keep the number of redundant records for the log instead.
    iters = {}

    errors = 0
    total_input_rows = total_output_rows = 0
    first_output_row = True

    for infile in args.infiles:
        reader = csv.reader(infile, dialect=args.dialect)

        header = []
        if args.watch_id_column < 1:
            header = next(reader)
            if "WatchID" not in header:
                print(f"WatchID is not present in the first line of "
                      f"{infile.name}. You have to use -w argument to specify "
                      f"it manually.", file=sys.stderr)
                exit(1)
            else:
                id_column = header.index("WatchID")
        else:
            id_column = args.watch_id_column - 1

        if args.iteration_column < 1:
            if "Iteration" in header:
                iter_column = header.index("Iteration")
            else:
                iter_column = -1
        else:
            iter_column = args.iteration_column - 1

        for row in reader:
            total_input_rows += 1

            try:
                _id = row[id_column]
                if iter_column >= 0:
                    prev_iter = iters.get(_id, -1)
                    if isinstance(prev_iter, str):
                        _iter = prev_iter
                    else:
                        _iter = optional_to_int(row[iter_column])
                        if not isinstance(_iter, str):
                            _iter = max(_iter, prev_iter)
                else:
                    _iter = iters.get(_id, 0) + 1
            except IndexError:
                errors += 1
                if args.in_place:
                    print(f"Error in the {total_input_rows} row: {row}\n\n"
                          f"Cannot continue with --in-place.", file=sys.stderr)
                    exit(1)
            else:
                iters[_id] = _iter

        outfile = (tempfile.NamedTemporaryFile("w", prefix=infile.name,
                                               delete=False)
                   if args.in_place else args.output)

        output_iters = {}

        writer = csv.writer(outfile, dialect=args.dialect,
                            quoting=csv.QUOTE_MINIMAL)

        if header and first_output_row or args.in_place:
            first_output_row = False
            writer.writerow(header)

        infile.seek(0)
        for row in reader:
            try:
                _id = row[id_column]
                if iter_column >= 0:
                    _iter = optional_to_int(row[iter_column])
                else:
                    _iter = output_iters.get(_id, 0) + 1
                    output_iters[_id] = _iter
            except IndexError:
                pass
            else:
                if _iter == iters.get(_id):
                    total_output_rows += 1
                    writer.writerow(row)

        if args.in_place:
            outfile.close()
            shutil.move(outfile.name, infile.name)

    if errors > 0:
        print(f"{errors} rows discarded due to errors.", file=sys.stderr)

    print(f"{total_output_rows} rows has written for {total_input_rows} input "
          f"rows.", file=sys.stderr)


def main():
    """
    The main entrypoint for the application
    """
    parser = argparse.ArgumentParser(
        description="Comma-Separated Values (CSV) utilities for pymongowatch")
    parser.set_defaults(func=lambda args: parser.print_help())

    sub_parsers = parser.add_subparsers(
        help="csv utility supports the following sub-commands")

    parser_aggr = sub_parsers.add_parser(
        "aggregate", aliases=["aggr"],
        help="aggregates logs with the same WatchID")
    parser_aggr.set_defaults(func=aggregate)

    parser_aggr.add_argument("infiles", metavar="<file>", nargs="+",
                             type=argparse.FileType("r"),
                             help="input CSV file")
    parser_aggr.add_argument("-i", "--in-place", action="store_true",
                             help="edit files in place")
    parser_aggr.add_argument("-o", "--output", metavar="<file>", default="-",
                             type=argparse.FileType("w"),
                             help="write output to <file> instead of stdout")
    parser_aggr.add_argument("-d", "--dialect", default="unix",
                             choices=["excel", "excel-tab", "unix"],
                             help="csv dialect")
    parser_aggr.add_argument("-w", "--watch-id-column", metavar="<n>",
                             default=-1, type=int,
                             help="Use the <n>th column as WatchID. If not "
                             "specified it trys to extract the column number "
                             "according to the first header line of the "
                             "files.")
    parser_aggr.add_argument("-t", "--iteration-column", metavar="<n>",
                             default=-1, type=int,
                             help="Use the <n>th column as Iteration to "
                             "detect the latest log. If not specified it "
                             "assumes the last log in the file is the latest "
                             "one unless watch id is not specified too, "
                             "CSV are available and an Iteration column is "
                             "named.")

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
