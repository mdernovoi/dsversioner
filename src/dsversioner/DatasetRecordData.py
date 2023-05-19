from pathlib import Path

import pandas


class DatasetRecordData:

    def __init__(self,
                 record_data: pandas.DataFrame = None):
        self._record_data = record_data

    @property
    def record_data(self):
        return self._record_data

    def to_csv(self,
               path: Path,
               header: bool,
               index_dimension_name: str) -> None:
        self._record_data.to_csv(
            path_or_buf=path,
            header=header,
            index=True,
            index_label=index_dimension_name
        )

    @classmethod
    def from_csv(cls,
                 path: Path,
                 header: int,
                 index_dimension_name: str):
        df = pandas.read_csv(
            filepath_or_buffer=path,
            header=header,
            index_col=index_dimension_name
        )
        return DatasetRecordData(record_data=df)

    def to_parquet(self,
                   path: Path) -> None:
        self._record_data.to_parquet(
            path=path,
            index=True)

    @classmethod
    def from_parquet(cls,
                     path: Path):
        df = pandas.read_parquet(
            path=path)
        return DatasetRecordData(record_data=df)
