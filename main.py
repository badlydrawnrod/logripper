import io
import pathlib
import zipfile

all_handles = []
all_files = []


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
        f = open_fn()
        bf = io.BufferedReader(f)
        all_handles.append(bf)
        all_files.append((full_path, encoding, io.TextIOWrapper(bf, encoding=encoding)))


def directory_storage_iterator(path):
    for full_path in sorted(path.glob("**/*")):
        if full_path.is_file():
            # TODO: remove this hack and go and figure out what the file is.
            if str(full_path).endswith(".zip"):
                with zipfile.ZipFile(full_path) as f:
                    zip_storage_iterator(full_path, f)
            else:
                opener(lambda: open(full_path, "rb"), full_path)


def zip_storage_iterator(parent_path, zf):
    for info in zf.infolist():
        # TODO: remove this hack and go and figure out what the file is.
        full_path = parent_path / info.filename
        if info.filename.endswith(".zip"):
            with zipfile.ZipFile(zf.open(info)) as f:
                zip_storage_iterator(full_path, f)
        else:
            opener(lambda: zf.open(info), full_path)


if __name__ == "__main__":
    path = pathlib.Path("samples")

    directory_storage_iterator(path)
    for path, encoding, stream in all_files:
        print(f"PATH {path} ENCODING {encoding}")
        cat(stream)

    while len(all_handles) > 0:
        all_handles.pop().close()
