from pathlib import Path

import pandas
import pyarrow as pa
import pyarrow.parquet as pq


class DatasetRecordData:

    def __init__(self,
                 record_data: pandas.DataFrame = None):
        self._record_data = record_data

    def to_csv(self,
               path: Path,
               write_header: bool,
               index_dimension_name: str) -> None:
        self._record_data.to_csv(
            path_or_buf=path,
            header=write_header,
            index=True,
            index_label=index_dimension_name
        )

    def to_parquet(self,
                   path: Path) -> None:
        self._record_data.to_parquet(
            path=path,
            index=True)
