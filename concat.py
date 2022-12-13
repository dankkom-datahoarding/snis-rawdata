import csv
import json
from itertools import chain
from pathlib import Path
from typing import Callable


def rename_columns_aguas_pluviais(name: str) -> str:
    """This function renames the identifier columns of the raw CSV files to
    standardize them
    """
    columns = {
        "Ano de Refererência": "ano_referencia",
        "Código do Município": "codigo_municipio_snis",
        "Código IBGE": "codigo_municipio",
        "Nome do Município": "nome_municipio",
        "UF": "sigla_uf",
        "Código do Estado": "codigo_uf",
        "Estado": "nome_uf",
        "Código da Região": "codigo_regiao",
        "Região": "nome_regiao",
        "Código da Microrregião": "codigo_microrregiao",
        "Microrregião": "nome_microrregiao",
        "Código da Mesorregião": "codigo_mesorregiao",
        "Setor Responsável": "setor_responsavel",
        "Natureza Jurídica": "natureza_juridica",
        "População Total": "municipio_populacao",
        "População Urbana": "municipio_populacao_urbana",
        "Faixa Populacional": "municipio_faixa_populacional",
        "Descrição Faixa": "municipio_faixa_populacional_descricao",
        "Capital": "municipio_capital",
        "Latitude": "municipio_latitude",
        "Longitude": "municipio_longitude",
        "Área [km^2]": "municipio_area_km2",
    }
    if name in columns:
        name = columns[name]
    return name


def rename_columns(name: str) -> str:
    """This function renames the identifier columns of the raw CSV files to
    standardize them
    """
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


def read_lines(filepath, columns_renamer: Callable) -> dict[str, str]:

    # The raw CSV files from SNIS are full of `null`s, so this function is
    # necessary to get rid of them and read data properly
    # Reference: https://stackoverflow.com/a/7895086/8429879
    def fix_nulls(s: str) -> str:
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


def clean_rows(data, skip_aggregation_rows=True, drop_ibge_column=True):
    AGGREGATION_ROW_START = ("TOTAL da AMOSTRA", "---")
    IBGE_COLUMN = "Código do IBGE"
    for row in data:
        if skip_aggregation_rows \
           and any(c in list(row.values())[0] for c in AGGREGATION_ROW_START):
            continue
        if drop_ibge_column and IBGE_COLUMN in row:
            row.pop(IBGE_COLUMN)
        yield row


def write(data, filepath):
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

    rawdir = Path(".")
    releasedir = Path("release")

    for group in metadata["datasets"]:
        for subgroup in metadata["datasets"][group]:
            print(group, subgroup)
            dirpath = rawdir / group / subgroup
            if group == "aguas-pluviais":
                columns_renamer = rename_columns_aguas_pluviais
            else:
                columns_renamer = rename_columns
            data = chain(
                *(
                    read_lines(f, columns_renamer)
                    for f in dirpath.glob("*.csv")
                )
            )
            destfilepath = releasedir / group / f"{subgroup}.csv"
            write(data, destfilepath)


if __name__ == "__main__":
    main()
