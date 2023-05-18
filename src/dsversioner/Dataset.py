from abc import ABCMeta, abstractmethod

from .DatasetMetadata import DatasetMetadata
from .DatasetVersion import DatasetVersion


# Types:
# Record data (tabular)
# Graph data
# Ordere data (timeseries)
class Dataset(metaclass=ABCMeta):
    def __init__(self,
                 name: str,
                 metadata: DatasetMetadata = None,
                 version: DatasetVersion = None
                 ) -> None:
        self._metadata = metadata
        self._name = name
        self._version = version

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
               amend: bool = False) -> None:
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
    def metadata(self):
        return self._metadata

    @property
    def version(self):
        return self._version

    @property
    def name(self):
        return self._name
