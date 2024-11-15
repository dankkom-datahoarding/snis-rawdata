import csv
import json
from itertools import chain
from pathlib import Path
from typing import Callable, Generator

AGGREGATION_ROW_START = ("TOTAL da AMOSTRA", "---")
IBGE_COLUMN = "Código do IBGE"
with open("metadata.json", encoding="utf-8") as f:
    METADATA = json.load(f)


def rename_columns_aguas_pluviais(name: str) -> str:
    """This function renames the identifier columns of the raw CSV files to
    standardize them
    """
    columns = METADATA["rename_columns_aguas_pluviais"]
    if name in columns:
        name = columns[name]
    return name


def rename_columns(name: str) -> str:
    """This function renames the identifier columns of the raw CSV files to
    standardize them
    """
    columns = METADATA["rename_columns"]
    if name in columns:
        name = columns[name]
    return name


def read_lines(filepath, columns_renamer: Callable) -> Generator[dict[str, str], None, None]:

    # The raw CSV files from SNIS are full of `null`s, so this function is
    # necessary to get rid of them and read data properly
    # Reference: https://stackoverflow.com/a/7895086/8429879
    def fix_nulls(s: str) -> Generator[str, None, None]:
        for line in s:
            yield line.replace("\0", "")

    with open(filepath, "r", encoding="latin-1") as f:
        reader = csv.reader(fix_nulls(f), delimiter=";")
        header = [columns_renamer(name) for name in next(reader)]
        for row in reader:
            if row:
                yield {
                    k: v.strip()
                    for k, v in zip(header, row)
                }


def read_directory(dirpath: Path, columns_renamer: Callable):
    readers = []
    for f in dirpath.glob("*.csv"):
        reader = read_lines(f, columns_renamer)
        readers.append(reader)
    return readers


def get_header(readers):
    header = []
    for reader in readers:
        columns = next(reader).keys()
        for column in columns:
            if column not in header and column != IBGE_COLUMN:
                header.append(column)
    return header


def clean_rows(data, skip_aggregation_rows=True, drop_ibge_column=True):
    for row in data:
        if skip_aggregation_rows \
           and any(c in list(row.values())[0] for c in AGGREGATION_ROW_START):
            continue
        if drop_ibge_column and IBGE_COLUMN in row:
            row.pop(IBGE_COLUMN)
        yield row


def write(data, filepath, header):
    if not filepath.parent.exists():
        filepath.parent.mkdir(parents=True)
    data = clean_rows(data)
    with open(filepath, "w", encoding="utf-8", newline="\n") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=header,
            delimiter=";",
            quoting=csv.QUOTE_ALL,
        )
        writer.writeheader()
        writer.writerows(data)


def main():

    rawdir = Path(".", "data")
    releasedir = Path("release")

    for group in METADATA["datasets"]:
        for subgroup in METADATA["datasets"][group]:
            print(group, subgroup)
            dirpath = rawdir / group / subgroup
            if group == "aguas-pluviais":
                columns_renamer = rename_columns_aguas_pluviais
            else:
                columns_renamer = rename_columns
            readers = read_directory(dirpath, columns_renamer)
            header = get_header(readers)
            data = chain(*readers)
            destfilepath = releasedir / group / f"{subgroup}.csv"
            write(data, destfilepath, header)


if __name__ == "__main__":
    main()
