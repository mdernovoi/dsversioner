"""
This module...
"""
import abc
from pathlib import Path


class JsonSerializable(abc.ABC):
    """
    This class...
    """

    @abc.abstractmethod
    def to_json(self) -> dict:
        """

        :return:
        """
        pass

    @classmethod
    @abc.abstractmethod
    def from_json(cls, json_dict: dict):  # TODO: uncomment after migration to python 3.11 -> Self:
        """

        :param json_dict:
        :return:
        """
        pass


class CSVSerializable(abc.ABC):
    """
    This class...
    """

    @abc.abstractmethod
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
        pass

    @classmethod
    @abc.abstractmethod
    def from_csv(cls,
                 path: Path,
                 # use first row as header
                 header_rows: int = 0,
                 index_dimension_name: str = 'id'):  # TODO: uncomment after migration to python 3.11 -> Self:
        """

        :param path:
        :param header_rows:
        :param index_dimension_name:
        :return:
        """
        pass


class ParquetSerializable(abc.ABC):
    """
    This class
    """

    @abc.abstractmethod
    def to_parquet(self,
                   path: Path) -> None:
        """

        :param path:
        :return:
        """
        pass

    @classmethod
    @abc.abstractmethod
    def from_parquet(cls,
                     path: Path):  # TODO: uncomment after migration to python 3.11 -> Self:
        """

        :param path:
        :return:
        """
        pass
