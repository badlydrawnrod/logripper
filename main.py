import io
import pathlib
import zipfile

all_streams = []


class TextStream:
    def __init__(self, owner, path, encoding):
        self.owner = owner
        self.reader = io.TextIOWrapper(self.owner, encoding=encoding)
        self.path = path
        self.encoding = encoding

    def readline(self):
        return self.reader.readline()


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
    for full_path in sorted(path.glob("**/*")):
        if full_path.is_file():
            # TODO: remove this hack and go and figure out what the file is.
            if str(full_path).endswith(".zip"):
                with zipfile.ZipFile(full_path) as f:
                    zip_storage_iterator(full_path, f)
            else:
                open_fn = lambda: open(full_path, "rb")
                encoding = guess_encoding(open_fn)
                if encoding:
                    all_streams.append(TextStream(io.BufferedReader(open_fn()), full_path, encoding))


def zip_storage_iterator(parent_path, zf):
    for info in zf.infolist():
        # TODO: remove this hack and go and figure out what the file is.
        full_path = parent_path / info.filename
        if info.filename.endswith(".zip"):
            with zipfile.ZipFile(zf.open(info)) as f:
                zip_storage_iterator(full_path, f)
        else:
            open_fn = lambda: zf.open(info)
            encoding = guess_encoding(open_fn)
            if encoding:
                all_streams.append(TextStream(io.BufferedReader(open_fn()), full_path, encoding))


if __name__ == "__main__":
    path = pathlib.Path("samples")

    directory_storage_iterator(path)

    read_data = True
    while read_data:
        read_data = False
        for stream in all_streams:
            line = stream.readline()
            if len(line) > 0:
                print(f"PATH {stream.path} ENCODING {stream.encoding} LINE {line.rstrip()}")
                read_data = True
