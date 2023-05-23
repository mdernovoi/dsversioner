"""
This module
"""
import abc
from abc import abstractmethod
from enum import Enum
from pathlib import Path
from typing import Type

from .DatasetMetadata import DatasetMetadata
from .DatasetRecordData import DatasetRecordData
from .DatasetVersion import DatasetVersion


class VersionStorage(abc.ABC):
    """
    This class
    """
    storage_identifier = "version_storage"

    @abstractmethod
    def init(self,
             dataset_name: str) -> None:
        """

        :param dataset_name:
        :return:
        """
        pass

    @abstractmethod
    def commit(self,
               dataset_name: str,
               dataset_version: DatasetVersion,
               amend: bool) -> DatasetVersion:
        """

        :param dataset_name:
        :param dataset_version:
        :param amend:
        :return:
        """
        pass

    @abstractmethod
    def pull(self,
             dataset_name: str,
             dataset_version: DatasetVersion) -> DatasetVersion:
        """

        :param dataset_name:
        :param dataset_version:
        :return:
        """
        pass

    @abstractmethod
    def drop(self,
             dataset_name: str) -> None:
        """

        :param dataset_name:
        :return:
        """
        pass


class MetadataStorage(abc.ABC):
    """
    This class
    """
    storage_identifier = "metadata_storage"

    @abstractmethod
    def init(self,
             dataset_name: str) -> None:
        """

        :param dataset_name:
        :return:
        """
        pass

    @abstractmethod
    def commit(self,
               dataset_name: str,
               dataset_version: DatasetVersion,
               dataset_metadata: DatasetMetadata,
               amend: bool) -> None:
        """

        :param dataset_name:
        :param dataset_version:
        :param dataset_metadata:
        :param amend:
        :return:
        """
        pass

    @abstractmethod
    def pull(self,
             dataset_name: str,
             dataset_version: DatasetVersion) -> DatasetMetadata:
        """

        :param dataset_name:
        :param dataset_version:
        :return:
        """
        pass

    @abstractmethod
    def drop(self,
             dataset_name: str) -> None:
        """

        :param dataset_name:
        :return:
        """
        pass


class RecordStorage(abc.ABC):
    """
    This class
    """
    storage_identifier = "record_storage"

    @abstractmethod
    def init(self,
             dataset_name: str) -> None:
        """

        :param dataset_name:
        :return:
        """
        pass

    @abstractmethod
    def commit(self,
               dataset_name: str,
               dataset_version: DatasetVersion,
               dataset_record_data: DatasetRecordData,
               dataset_metadata: DatasetMetadata,
               working_directory: Path) -> None:
        """

        :param dataset_name:
        :param dataset_version:
        :param dataset_record_data:
        :param dataset_metadata:
        :param working_directory:
        :return:
        """
        pass

    @abstractmethod
    def pull(self,
             dataset_name: str,
             dataset_version: DatasetVersion,
             dataset_metadata: DatasetMetadata,
             dataset_record_data: DatasetRecordData,
             working_directory: Path) -> DatasetRecordData:
        """

        :param dataset_record_data:
        :param dataset_name:
        :param dataset_version:
        :param dataset_metadata:
        :param working_directory:
        :return:
        """
        pass

    @abstractmethod
    def drop(self,
             dataset_name: str) -> None:
        """

        :param dataset_name:
        :return:
        """
        pass


class RecordStorageFormats(Enum):
    """
    This enum
    """
    CSV = 1
    PARQUET = 2


class ObjectStorage(abc.ABC):
    """
    This class
    """
    storage_identifier = "object_storage"

    @abstractmethod
    def init(self,
             dataset_name: str) -> None:
        """

        :param dataset_name:
        :return:
        """
        pass

    @abstractmethod
    def commit(self,
               dataset_name: str,
               dataset_version: DatasetVersion,
               dataset_record_data: DatasetRecordData,
               dataset_metadata: DatasetMetadata,
               working_directory: Path) -> None:
        """

        :param dataset_name:
        :param dataset_version:
        :param dataset_record_data:
        :param dataset_metadata:
        :param working_directory:
        :return:
        """
        pass

    @abstractmethod
    def pull(self,
             dataset_name: str,
             dataset_version: DatasetVersion,
             dataset_metadata: DatasetMetadata,
             dataset_record_data: DatasetRecordData,
             working_directory: Path) -> None:
        """

        :param dataset_name:
        :param dataset_version:
        :param dataset_metadata:
        :param dataset_record_data:
        :param working_directory:
        :return:
        """
        pass

    @abstractmethod
    def drop(self,
             dataset_name: str) -> None:
        """

        :param dataset_name:
        :return:
        """
        pass


class FileSystemStorage(abc.ABC):
    """
    This class
    """

    @property
    @abstractmethod
    def root_path(self) -> Path:
        """

        :return:
        """
        pass

# class S3Storage(Storage):
#     pass
#
#
# class MinioStorage(S3Storage):
#     pass
