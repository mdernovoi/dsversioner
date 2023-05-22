
"""
This module...
"""

from abc import ABCMeta, abstractmethod
from pathlib import Path

from .DatasetMetadata import DatasetMetadata
from .DatasetVersion import DatasetVersion


class Dataset(metaclass=ABCMeta):
    """
    # Future Types:
    # Record data (tabular)
    # Graph data
    # Ordere data (timeseries)
    """

    @abstractmethod
    def init(self) -> None:
        """


        :return:
        """
        pass

    @abstractmethod
    def add(self) -> None:
        """

        :return:
        """
        pass

    @abstractmethod
    def commit(self,
               version: DatasetVersion = None,
               amend: bool = False) -> DatasetVersion:
        """

        :param version:
        :param amend:
        :return:
        """
        pass

    @abstractmethod
    def pull(self,
             version: DatasetVersion = None) -> None:
        """

        :param version:
        :return:
        """
        pass

    @abstractmethod
    def drop(self) -> None:
        """

        :return:
        """
        pass

    @property
    @abstractmethod
    def version(self) -> DatasetVersion:
        """

        :return:
        """
        pass

    @property
    @abstractmethod
    def working_directory(self) -> Path:
        """

        :return:
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """

        :return:
        """
        pass

    @property
    @abstractmethod
    def metadata(self) -> DatasetMetadata:
        """

        :return:
        """
        pass
