import contextlib
import io
import os
import pathlib
import zipfile


def cat(f):
    for line in f:
        line = line.rstrip()
        print(line)


@contextlib.contextmanager
def open_as_text(open_fn, lines=10):
    """Given an open function, attempts to open a file as a text file with a sensible encoding."""
    encodings = ["utf-16", "utf-8", "ascii"]
    for e in encodings:
        with open_fn() as f:
            try:
                with io.BufferedReader(f) as bf:
                    wrapper = io.TextIOWrapper(bf, encoding=e)
                    wrapper.readlines(lines)
            except UnicodeError:
                continue

        # At this point, we know what the encoding is.
        with open_fn() as f:
            with io.BufferedReader(f) as bf:
                yield e, io.TextIOWrapper(bf, encoding=e)
        break


def directory_storage_iterator(path):
    for filename in os.listdir(path):
        full_path = path / filename
        # TODO: remove this hack and go and figure out what the file is.
        if filename.endswith(".zip"):
            with zipfile.ZipFile(full_path) as f:
                yield from zip_storage_iterator(full_path, f)
        if not filename.endswith(".zip"):
            yield full_path, lambda: open(full_path, "rb")


def zip_storage_iterator(parent_path, zf):
    for info in zf.infolist():
        # TODO: remove this hack and go and figure out what the file is.
        full_path = parent_path / info.filename
        if info.filename.endswith(".zip"):
            with zipfile.ZipFile(zf.open(info)) as f:
                yield from zip_storage_iterator(full_path, f)
        else:
            yield full_path, lambda: zf.open(info)


if __name__ == "__main__":
    path = pathlib.Path("samples")

    for file_path, opener in directory_storage_iterator(path):
        with open_as_text(opener) as (e, f):
            print(f"--- PATH: {file_path} ENCODING: {e}")
            cat(f)
