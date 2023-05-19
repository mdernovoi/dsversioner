from abc import ABCMeta, abstractmethod

from .DatasetMetadata import DatasetMetadata
from .DatasetVersion import DatasetVersion


# Types:
# Record data (tabular)
# Graph data
# Ordere data (timeseries)
class Dataset(metaclass=ABCMeta):

    @abstractmethod
    def init(self) -> None:
        """
        :return:
        :raises DatasetExistsException:
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
        :raises DatasetDoesNotExistException
        :raises DatasetVersionExistsException
        :raises NothingToCommitException
        """
        pass

    @abstractmethod
    def pull(self,
             version: DatasetVersion = None) -> None:
        """

        :param version:
        :return:
        :raises DatasetDoesNotExistException
        :raises DatasetVersionDoesNotExistException:
        :raises NothingToPullException
        """
        pass

    @abstractmethod
    def drop(self) -> None:
        """

        :return:
        :raises DatasetDoesNotExistException
        """
        pass

    @property
    @abstractmethod
    def version(self) -> DatasetVersion:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def metadata(self) -> DatasetMetadata:
        pass
