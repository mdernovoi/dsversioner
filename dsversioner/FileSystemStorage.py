"""
This module
"""
import abc

from pathlib import Path


class FileSystemStorage(abc.ABC):
    """
    This class
    """

    @property
    @abc.abstractmethod
    def root_path(self) -> Path:
        """

        :return:
        """
        pass
