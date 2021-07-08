import argparse
import io
import pathlib
import re
import tarfile
import zipfile

import dateutil.parser

# TODO: handle other time formats.
# TODO: associate the time format with the stream so that we don't have to keep working it out.
# TODO: add a requirements.txt, or whatever Python uses these days.
# TODO: better error handling all round.
# TODO: add a licence.

# Compile a regular expression that will match an ISO timestamp.
iso = r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.(\d{9}|\d{6}|\d{3}))?(Z|[+-]\d{4}|[+-]\d{2}(:\d{2})?)?"
iso_re = re.compile(iso)


class LogStream:
    def __init__(self, owner, path, encoding):
        self.reader = io.TextIOWrapper(owner, encoding=encoding)
        self.path = path
        self.current_line = ""
        self.current_time = None
        # TODO: read ahead a little, in case this stream has headers that don't have timestamps.
        self.peekline()

    def peekline(self):
        if len(self.current_line) == 0:
            self.current_line = self.reader.readline()
            if matched := iso_re.match(self.current_line):
                start, end = matched.span()
                self.current_time = dateutil.parser.isoparse(self.current_line[start:end])
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
                    with io.BufferedReader(f) as bf:
                        wrapper = io.TextIOWrapper(bf, encoding=e)
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
    if encoding := guess_encoding(open_fn):
        stream = LogStream(io.BufferedReader(open_fn()), full_path, encoding)
        if stream.current_time is not None:
            return stream


def open_as_zip(open_token):
    try:
        return zipfile.ZipFile(open_token())
    except zipfile.BadZipfile:
        pass
    except AttributeError:
        pass


def open_as_tar(open_token):
    try:
        return tarfile.open(open_token())
    except tarfile.TarError:
        pass
    except TypeError:
        pass
    except ValueError:
        pass


def recurse(open_fn, full_path, open_token):
    if f := open_as_zip(open_token):
        try:
            return recurse_in_zip(full_path, f)
        finally:
            f.close()
    elif f := open_as_tar(open_token):
        try:
            return recurse_in_tar(full_path, f)
        finally:
            f.close()
    elif stream := open_as_log(open_fn, full_path):
        return [stream]
    return []


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
        if full_path.is_file():
            result.extend(recurse(lambda: open(full_path, "rb"), full_path, lambda: full_path))
    return result


def remove_finished_streams(streams):
    result = []
    for stream in streams:
        if stream.peekline() and stream.current_time is not None:
            result.append(stream)
        else:
            stream.close()
    result.sort(key=lambda s: s.current_time)
    return result


def main(filepaths):
    streams = recurse_in_filesystem(filepaths)

    while streams := remove_finished_streams(streams):
        oldest = streams[0]
        line = oldest.readline()
        timestamp = oldest.current_time.isoformat()
        line = line.strip()
        print(f"{timestamp}, {line}, {oldest.path}")

        # Read any lines from the oldest stream that don't have a timestamp and output them as if they have the same
        # timestamp as the previous line.
        line = oldest.peekline()
        while len(line) > 0 and oldest.current_time is None:
            line = oldest.readline()
            line = line.strip()
            print(f"{timestamp}, {line}, {oldest.path}")
            line = oldest.peekline()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chomp through log files.")
    parser.add_argument("paths", metavar="file/dir", type=str, nargs="*", help="filenames or directories to search")
    args = parser.parse_args()

    main(pathlib.Path(p) for p in args.paths)
