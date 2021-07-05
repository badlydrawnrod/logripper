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


if __name__ == "__main__":
    path = pathlib.Path("samples")

    for filename in os.listdir(path):
        if not filename.endswith(".zip"):
            full_path = path / filename
            with open_as_text(lambda: open(full_path, "rb")) as (e, f):
                print(f"Encoding = {e}")
                cat(f)

    zipname = path / "samples.zip"
    with zipfile.ZipFile(zipname) as zf:
        for filename in zf.namelist():
            full_path = zipname / filename
            with open_as_text(lambda: zf.open(filename)) as (e, f):
                print(f"Encoding = {e}")
                cat(f)
