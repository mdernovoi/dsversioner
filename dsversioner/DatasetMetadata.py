"""
This module...
"""

import abc


class PrivateDatasetMetadata(abc.ABC):
    """
    This class...
    """
    pass


class PublicDatasetMetadata(abc.ABC):
    """
    This class...
    """
    pass


class DatasetMetadata(abc.ABC):
    """
    This class...
    """

    @property
    @abc.abstractmethod
    def private_metadata(self) -> PrivateDatasetMetadata:
        """

        :return:
        """
        pass

    @property
    @abc.abstractmethod
    def public_metadata(self) -> PublicDatasetMetadata:
        """

        :return:
        """
        pass


class PublicKeyValueDatasetMetadata(PublicDatasetMetadata):
    """
    This class..
    """

    def __init__(self,
                 data: dict = None) -> None:
        self._data = data
        if self._data is None:
            self._data = dict()

    @property
    def data(self) -> dict:
        """

        :return:
        """
        return self._data

    def get_value_by_key(self,
                         key: str):
        """

        :param key:
        :return:
        """
        return self._data[key]

    def set_value_by_key(self,
                         key: str,
                         value) -> None:
        """

        :param key:
        :param value:
        :return:
        """
        self._data[key] = value
