# Copyright (c) 2021 Rod Hyde (@badlydrawnrod)
#
# This software is provided "as-is", without any express or implied warranty. In no event
# will the authors be held liable for any damages arising from the use of this software.
#
# Permission is granted to anyone to use this software for any purpose, including commercial
# applications, and to alter it and redistribute it freely, subject to the following restrictions:
#
# 1. The origin of this software must not be misrepresented; you must not claim that you
# wrote the original software. If you use this software in a product, an acknowledgment
# in the product documentation would be appreciated but is not required.
#
# 2. Altered source versions must be plainly marked as such, and must not be misrepresented
# as being the original software.
#
# 3. This notice may not be removed or altered from any source distribution.

import argparse
import datetime
import io
import logging
import pathlib
import re
import tarfile
import zipfile

import dateutil.parser

# TODO: let the user pick the default timezone if none is given (currently defaults to local timezone, but UTC might be
#   a better option in many circumstances.
# TODO: let the user pick the output timezone (currently defaults to local timezone).
# TODO: customise the output format
#   e.g.
#   - display / don't display timestamps
#   - display / don't display paths
#   - output as CSV
# TODO: allow the user to specify a file inside an archive if they know the name, e.g., my.zip/my_file.txt
# TODO: make it extensible in terms of archive formats, etc, that it can handle (by registering handlers for filetypes).
# TODO: allow it to select file formats by extension as well as by inspection (if that makes sense).
# TODO: allow the loglevel to be set via the command line.
# TODO: publish on GitHub.
# TODO: add a README.md.
# TODO: publish on PyPI.
# TODO: handle other (non ISO) time formats when parsing.
# TODO: associate the time format with the stream so that we don't have to keep working it out.
# TODO: better error handling all round (e.g., if a zip file has an unsupported compression method).
# TODO: faster, better, timestamp filtering.
# TODO: allow the user to register handlers for events, e.g., on log line found, on file found, etc.
# TODO: allow actions when a line is output, e.g., if the line contains XXX then print YYY above it.
# TODO: restrict *file* timestamps to a particular range, e.g., ignore files created before last year or that are less
#   than 3 months old.
# TODO: the equivalent of tail -f across all (toplevel?) logs.

# Date ranges.
min_date = datetime.datetime(datetime.MINYEAR, 1, 1, tzinfo=datetime.timezone.utc)
max_date = datetime.datetime(datetime.MAXYEAR, 12, 31, 23, 59, tzinfo=datetime.timezone.utc)

# Default timezone.
default_tz = None

# Compile a regular expression that will match an ISO timestamp.
iso = r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.(\d{9}|\d{6}|\d{3}))?(Z|[+-]\d{4}|[+-]\d{2}(:\d{2})?)?"
iso_re = re.compile(iso)

# If we attempt to open a stream and get one of these exceptions then we just abandon that stream and move on.
non_fatal_exceptions = (
    zipfile.BadZipfile,
    tarfile.TarError,
    NotImplementedError,
    TypeError,
    ValueError,
    AttributeError
)

archive_handlers = []
ignore_list = []


def parse_date(s):
    return dateutil.parser.parse(s).astimezone(tz=default_tz)


class LogStream:
    def __init__(self, stream, path, encoding, lines=10):
        self.reader = io.TextIOWrapper(stream, encoding=encoding)
        self.path = path
        self.current_line = ""
        self.current_time = None

        # Look for a line starting with a timestamp in the first few lines of the stream.
        self.peekline()
        while len(self.current_line) > 0 and self.current_time is None and lines > 0:
            self.readline()
            self.peekline()
            lines -= 1

    def peekline(self):
        if len(self.current_line) == 0:
            self.current_line = self.reader.readline()
            # Use search() instead of match() as the timestamp may not be the first thing on the line.
            if matched := iso_re.search(self.current_line):
                start, end = matched.span()
                self.current_time = parse_date(self.current_line[start:end])
                self.current_line = self.current_line[end:]
            else:
                self.current_time = None

        return self.current_line

    def readline(self):
        result = self.current_line if len(self.current_line) > 0 else self.peekline()
        self.current_line = ""
        return result

    def close(self):
        self.reader.close()


def guess_encoding(open_fn, lines=10):
    encodings = ["utf-16", "utf-8", "ascii"]
    try:
        for e in encodings:
            with open_fn() as f:
                try:
                    with io.BufferedReader(f) as br:
                        wrapper = io.TextIOWrapper(br, encoding=e)
                        wrapper.readlines(lines)
                        return e
                except UnicodeError:
                    continue
    except AttributeError as e:
        if str(e) != "__enter__":
            raise
        # If we get here then open_fn() returned most likely returned None. This can happen, e.g., when looking in a
        # tar file for which there is an entry, but nothing extractable for that entry.


def open_as_log(open_fn, full_path):
    try:
        if encoding := guess_encoding(open_fn):
            stream = LogStream(io.BufferedReader(open_fn()), full_path, encoding)
            if stream.current_time is not None:
                return stream
    except non_fatal_exceptions:
        pass


def open_as_zip(open_token):
    try:
        return zipfile.ZipFile(open_token())
    except non_fatal_exceptions:
        pass


def open_as_tar(open_token):
    try:
        return tarfile.open(open_token())
    except non_fatal_exceptions:
        pass


def recurse(open_fn, full_path, open_token):
    if is_ignored(full_path):
        return []

    for filetype, opener, actor in archive_handlers:
        if f := opener(open_token):
            try:
                logging.info(f"processing {filetype} file {full_path}")
                return actor(full_path, f)
            finally:
                f.close()

    if stream := open_as_log(open_fn, full_path):
        logging.info(f"found log file {full_path}")
        return [stream]

    logging.debug(f"ignoring {full_path}")
    return []


def is_ignored(path):
    for item in ignore_list:
        if path.match(item):
            logging.info(f"filtering out {path}")
            return True
    return False


def recurse_in_zip(parent_path, zf):
    result = []
    for info in zf.infolist():
        full_path = parent_path / info.filename
        result.extend(recurse(lambda: zf.open(info), full_path, lambda: zf.open(info)))
    return result


def recurse_in_tar(parent_path, tf):
    result = []
    for info in tf.getmembers():
        full_path = parent_path / info.name
        result.extend(recurse(lambda: tf.extractfile(info), full_path, lambda: tf.extractfile(info)))
    return result


def recurse_in_filesystem(filepaths):
    # Determine up-front which files we're going to look in, and get rid of any duplicates.
    paths = set()
    for path in filepaths:
        if path.is_file():
            paths.add(path)
        elif path.is_dir():
            paths.update(path.glob("**/*"))
    paths = sorted(paths)

    result = []
    for full_path in paths:
        if is_ignored(full_path):
            continue
        if full_path.is_dir():
            logging.info(f"processing directory {full_path}")
        elif full_path.is_file():
            result.extend(recurse(lambda: open(full_path, "rb"), full_path, lambda: full_path))
    return result


def remove_finished_streams(streams):
    result = []
    for stream in streams:
        if stream.peekline() and stream.current_time is not None:
            result.append(stream)
        else:
            logging.info(f"Closing {stream.path}")
            stream.close()
    result.sort(key=lambda s: s.current_time)
    return result


def iterate_through_logs(streams):
    while streams := remove_finished_streams(streams):
        oldest = streams[0]
        line = oldest.readline()
        timestamp = oldest.current_time.isoformat()
        line = line.strip()
        yield timestamp, line, oldest.path

        # Read any lines from the oldest stream that don't have a timestamp and output them as if they have the same
        # timestamp as the previous line.
        line = oldest.peekline()
        while len(line) > 0 and oldest.current_time is None:
            line = oldest.readline()
            yield timestamp, line, oldest.path
            line = oldest.peekline()


def rip(filepaths, start_time, end_time):
    streams = recurse_in_filesystem(filepaths)
    for timestamp, line, path in iterate_through_logs(streams):
        line = line.strip()
        timestamp = parse_date(timestamp)
        if start_time <= timestamp < end_time:
            print(f"{timestamp}, {line}, {path}")


def parse_command_line():
    # Parse the command line.
    parser = argparse.ArgumentParser(prog="python -m logripper", description="Rip through log files in time order.",
                                     epilog="Happy ripping.")

    # Time-related arguments.
    time_group = parser.add_argument_group("time related arguments")
    time_group.add_argument("--time-from", "--from", "--starttime",
                            dest="start_time", metavar="<timestamp>", type=str, action="store",
                            help="the timestamp to display from. Lines before this time will not be displayed.")
    time_group.add_argument("--time-to", "--to", "--endtime",
                            dest="end_time", metavar="<timestamp>", type=str, action="store",
                            help="the timestamp to display until. Lines from this time onward will not be displayed.")

    # Filtering by name.
    ignore_group = parser.add_argument_group("filtering")
    ignore_group.add_argument("--ignore", "--ignored",
                              dest="ignored", metavar="<glob>", type=str, action="append",
                              help="ignore files and directories that match this pattern")

    # Arguments that control archive handling.
    archive_group = parser.add_argument_group("archive handling")
    archive_group.add_argument("--tars", metavar="yes|no", dest="tars", choices=["yes", "no"], default="yes",
                               help="descend into tar files (default=yes).")
    archive_group.add_argument("--zips", metavar="yes|no", dest="zips", choices=["yes", "no"], default="yes",
                               help="descend into zips (default=yes)")

    # The paths to inspect for log files.
    parser.add_argument("paths", metavar="<file|dir>", type=str, nargs="*", help="files or directories to search")

    return parser.parse_args()


if __name__ == "__main__":
    # Enable basic logging.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%FT%H:%M:%S%z")

    args = parse_command_line()

    # Turn the parsed args into a configuration that we can understand.
    args.start_time = parse_date(args.start_time) if args.start_time else min_date
    args.end_time = parse_date(args.end_time) if args.end_time else max_date

    # Register archive handlers.
    if args.zips == "yes":
        archive_handlers.append(("zip", open_as_zip, recurse_in_zip))
    if args.tars == "yes":
        archive_handlers.append(("tar", open_as_tar, recurse_in_tar))

    # Which files to ignore.
    if args.ignored:
        for item in args.ignored:
            ignore_list.append(item)

    # Default to the current directory if no paths were specified.
    args.paths = args.paths if args.paths else ["."]

    # Examine the logs, in timeline order.
    rip((pathlib.Path(p) for p in args.paths), args.start_time, args.end_time)
