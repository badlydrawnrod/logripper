import io
import os
import pathlib
import zipfile


def cat(f):
    for line in f:
        line = line.rstrip()
        print(line)


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


def opener(open_fn, full_path):
    encoding = guess_encoding(open_fn)
    if encoding:
        with open_fn() as f:
            with io.BufferedReader(f) as bf:
                yield full_path, encoding, io.TextIOWrapper(bf, encoding=encoding)


def directory_storage_iterator(path):
    for full_path in sorted(path.glob("**/*")):
        if full_path.is_file():
            # TODO: remove this hack and go and figure out what the file is.
            if str(path).endswith(".zip"):
                with zipfile.ZipFile(full_path) as f:
                    yield from zip_storage_iterator(full_path, f)
            else:
                yield from opener(lambda: open(full_path, "rb"), full_path)


def zip_storage_iterator(parent_path, zf):
    for info in zf.infolist():
        # TODO: remove this hack and go and figure out what the file is.
        full_path = parent_path / info.filename
        if info.filename.endswith(".zip"):
            with zipfile.ZipFile(zf.open(info)) as f:
                yield from zip_storage_iterator(full_path, f)
        else:
            yield from opener(lambda: zf.open(info), full_path)


if __name__ == "__main__":
    path = pathlib.Path("samples")

    for file_path, encoding, stream in directory_storage_iterator(path):
        print(f"PATH: {file_path}, ENCODING: {encoding}")
        # Now we have a seekable, buffered text stream, so we can sample a little of it to guess the timestamp format,
        # then happily rewind it.
        cat(stream)
