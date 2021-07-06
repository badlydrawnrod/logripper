import datetime
import dateutil.parser
import io
import pathlib
import re
import zipfile

# Compile a regular expression that will match an ISO timestamp.
iso = r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.(\d{9}|\d{6}|\d{3}))?(Z|[+-]\d{4}|[+-]\d{2}(:\d{2})?)?"
iso_re = re.compile(iso)


class TextStream:
    def __init__(self, owner, path, encoding):
        self.owner = owner
        self.reader = io.TextIOWrapper(self.owner, encoding=encoding)
        self.path = path
        self.current_line = ""
        self.current_time = None

    def peekline(self):
        if len(self.current_line) == 0:
            self.current_line = self.reader.readline()
            matched = iso_re.match(self.current_line)
            if matched:
                start, end = matched.span()
                self.current_time = dateutil.parser.parse(self.current_line[start:end])
                self.current_line = self.current_line[end:]
            else:
                self.current_time = None

        return self.current_line

    def readline(self):
        result = self.current_line if len(self.current_line) > 0 else self.peekline()
        self.current_line = ""
        return result


def guess_encoding(open_fn, lines=10):
    encodings = ["utf-16", "utf-8", "ascii"]
    for e in encodings:
        with open_fn() as f:
            try:
                with io.BufferedReader(f) as bf:
                    wrapper = io.TextIOWrapper(bf, encoding=e)
                    wrapper.readlines(lines)
                    return e
            except UnicodeError:
                continue


def directory_storage_iterator(path):
    result = []
    for full_path in sorted(path.glob("**/*")):
        if full_path.is_file():
            # TODO: remove this hack and go and figure out what the file is.
            if str(full_path).endswith(".zip"):
                with zipfile.ZipFile(full_path) as f:
                    result.extend(zip_storage_iterator(full_path, f))
            else:
                open_fn = lambda: open(full_path, "rb")
                encoding = guess_encoding(open_fn)
                if encoding:
                    result.append(TextStream(io.BufferedReader(open_fn()), full_path, encoding))
    return result


def zip_storage_iterator(parent_path, zf):
    result = []
    for info in zf.infolist():
        # TODO: remove this hack and go and figure out what the file is.
        full_path = parent_path / info.filename
        if info.filename.endswith(".zip"):
            with zipfile.ZipFile(zf.open(info)) as f:
                result.extend(zip_storage_iterator(full_path, f))
        else:
            open_fn = lambda: zf.open(info)
            encoding = guess_encoding(open_fn)
            if encoding:
                result.append(TextStream(io.BufferedReader(open_fn()), full_path, encoding))
    return result


if __name__ == "__main__":
    path = pathlib.Path(".")

    text_streams = directory_storage_iterator(path)

    # Advance each stream until there are no streams left.
    while len(text_streams) > 0:
        # Find the stream with the oldest timestamp, and remove streams that don't have timestamps.
        oldest = None
        streams = []
        for stream in text_streams:
            line = stream.peekline()
            if stream.current_time is not None:
                streams.append(stream)
                if oldest is None or stream.current_time < oldest.current_time:
                    oldest = stream
        text_streams = streams

        if oldest is not None:
            line = oldest.readline()
            ts = oldest.current_time.isoformat()
            if len(line) > 0:
                line = line.strip()
                print(f"{ts}, {line}, {oldest.path}")

                # Read any lines that don't have a timestamp and account for them as if they're part of the previous
                # line.
                line = oldest.peekline()
                while len(line) > 0 and oldest.current_time is None:
                    line = oldest.readline()
                    line = line.strip()
                    print(f"{ts}, {line}, {oldest.path}")
                    line = oldest.peekline()
