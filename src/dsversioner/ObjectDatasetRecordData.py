"""
This module...
"""
import abc
from pathlib import Path

import pandas

from .DatasetRecordData import DatasetRecordData, PandasDatasetRecordData
from .Serializable import CSVSerializable, ParquetSerializable


class ObjectDatasetRecordData(DatasetRecordData, CSVSerializable, ParquetSerializable):
    """
    This class
    """

    @property
    @abc.abstractmethod
    def record_data(self):
        pass

    @abc.abstractmethod
    def to_csv(self, path: Path, write_header: bool = True, write_index_dimension: bool = True,
               index_dimension_name: str = 'id') -> None:
        pass

    @classmethod
    @abc.abstractmethod
    def from_csv(cls, path: Path, header_rows: int = 0, index_dimension_name: str = 'id'):
        pass

    @abc.abstractmethod
    def to_parquet(self, path: Path) -> None:
        pass

    @classmethod
    @abc.abstractmethod
    def from_parquet(cls, path: Path):
        pass


class PandasObjectDatasetRecordData(PandasDatasetRecordData, ObjectDatasetRecordData):
    """
    This class
    """

    def __init__(self,
                 record_data: pandas.DataFrame = None):
        self._record_data = record_data

    @property
    def record_data(self) -> pandas.DataFrame:
        """

        :return:
        """
        return self._record_data

    def to_csv(self,
               path: Path,
               write_header: bool = True,
               write_index_dimension: bool = True,
               index_dimension_name: str = 'id') -> None:
        """

        :param path:
        :param write_header:
        :param write_index_dimension:
        :param index_dimension_name:
        :return:
        """
        self._record_data.to_csv(
            path_or_buf=path,
            header=write_header,
            index=write_index_dimension,
            index_label=index_dimension_name
        )

    @classmethod
    def from_csv(cls,
                 path: Path,
                 header_rows: int = 1,
                 index_dimension_name: str = 'id'):  # TODO: uncomment after migration to python 3.11 -> Self:
        """

        :param path:
        :param header_rows:
        :param index_dimension_name:
        :return:
        """
        df = pandas.read_csv(
            filepath_or_buffer=path,
            header=header_rows,
            index_col=index_dimension_name
        )
        return PandasObjectDatasetRecordData(record_data=df)

    def to_parquet(self,
                   path: Path) -> None:
        """

        :param path:
        :return:
        """
        self._record_data.to_parquet(
            path=path,
            index=True)

    @classmethod
    def from_parquet(cls,
                     path: Path):  # TODO: uncomment after migration to python 3.11 -> Self:
        """

        :param path:
        :return:
        """
        df = pandas.read_parquet(
            path=path)

        return PandasObjectDatasetRecordData(record_data=df)
