"""
This module...
"""
import abc


class DatasetVersion(abc.ABC):
    """
    This class...
    """

    @property
    @abc.abstractmethod
    def id(self) -> int:
        """

        :return:
        """
        pass

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """

        :return:
        """
        pass

    @classmethod
    @abc.abstractmethod
    def from_id(cls,
                id: int):  # TODO: uncomment after migration to python 3.11 -> Self:
        """

        :param id:
        :return:
        """
        pass
