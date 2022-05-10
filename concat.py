import csv
import json
from itertools import chain
from pathlib import Path


def read_lines(filepath, rename_columns=True) -> dict[str, str]:

    # The raw CSV files from SNIS are full of `null`s, so this function is
    # necessary to get rid of them and read data properly
    # Reference: https://stackoverflow.com/a/7895086/8429879
    def fix_nulls(s: str) -> str:
        for line in s:
            yield line.replace("\0", "")

    # This function renames the identifier columns of the raw CSV files to
    # standardize them
    def rename_column(name: str) -> str:
        columns = {
            "Ano de Referência": "ano_referencia",
            "Região": "nome_regiao",
            "Estado": "sigla_uf",
            "Código do Município": "codigo_municipio",
            "Município": "nome_municipio",
            "Região Metropolitana": "nome_regiao_metropolitana",
            "Código do Prestador": "codigo_prestador",
            "Prestador": "nome_prestador",
            "Prestadores": "nome_prestador",
            "Sigla do Prestador": "sigla_prestador",
            "Abrangência": "abrangencia",
            "Natureza jurídica": "natureza_juridica",
            "Natureza Jurídica": "natureza_juridica",
            "Tipo de serviço": "tipo_servico",
            "Tipo de Serviço": "tipo_servico",
            "Serviços": "tipo_servico",
            "Serviço Prestado": "tipo_servico",
            "Unidade": "codigo_unidade",
            "Unidades": "codigo_unidade",
            "Nome da Unidade": "nome_unidade",
            "Tipo da Unidade": "tipo_unidade",
            "Operador": "operador",
        }
        if name in columns:
            name = columns[name]
        return name

    with open(filepath, "r", encoding="latin-1") as f:
        reader = csv.reader(fix_nulls(f), delimiter=";")
        if rename_columns:
            header = [rename_column(name) for name in next(reader)]
        else:
            header = next(reader)
        for row in reader:
            if row:
                yield {
                    k: v.strip()
                    for k, v in zip(header, row)
                }


def clean_rows(data, skip_aggregation_rows=True, drop_ibge_column=True):
    AGGREGATION_ROW_START = "TOTAL da AMOSTRA:"
    IBGE_COLUMN = "Código do IBGE"
    for row in data:
        if skip_aggregation_rows \
           and AGGREGATION_ROW_START in row["codigo_municipio"]:
            continue
        if drop_ibge_column and IBGE_COLUMN in row:
            row.pop(IBGE_COLUMN)
        yield row


def write(data, filepath, clean=True):
    if not filepath.parent.exists():
        filepath.parent.mkdir(parents=True)
    data = clean_rows(data)
    first_row = next(data)
    header = first_row.keys()
    with open(filepath, "w", encoding="utf-8", newline="\n") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=header,
            delimiter=";",
            quoting=csv.QUOTE_ALL,
        )
        writer.writeheader()
        writer.writerow(first_row)
        writer.writerows(data)


def main():

    with open("metadata.json", encoding="utf-8") as f:
        metadata = json.load(f)

    datarawdir = Path("data-raw")
    datadir = Path("data")

    for group in metadata["datasets"]:
        for subgroup in metadata["datasets"][group]:
            print(group, subgroup)
            dirpath = datarawdir / group / subgroup
            data = chain(*(read_lines(f) for f in dirpath.glob("*.csv")))
            destfilepath = datadir / group / f"{subgroup}.csv"
            write(data, destfilepath)


if __name__ == "__main__":
    main()
