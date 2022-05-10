# Raw data files from *SNIS - Série Histórica*

Data source: [SNIS - Série Histórica](http://app4.mdr.gov.br/serieHistorica/)

To read about **SNIS - Série Histórica** there is a copy of its description in [metadata.md](metadata.md) in this repository.

## License

This SNIS-DATA is made available under the Open Database License: http://opendatacommons.org/licenses/odbl/1.0/. Any rights in individual contents of the database are licensed under the Database Contents License: http://opendatacommons.org/licenses/dbcl/1.0/

## Scripts

`concat.py` is a pure Python script to concatenate datasets in the repository, which are partitioned based on reference year, into one file each. It also makes some small data cleanup and column renaming.
