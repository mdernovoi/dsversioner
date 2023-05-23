"""
This module
"""

import abc
from abc import abstractmethod
from pathlib import Path
from typing import Type

from .ObjectDatasetMetadata import ObjectDatasetMetadata
from .ObjectDatasetRecordData import ObjectDatasetRecordData
from .ObjectDatasetVersion import ObjectDatasetVersion
from .Storage import MetadataStorage, VersionStorage, ObjectStorage, RecordStorage


class ObjectDatasetStorage(abc.ABC):
    """
    This class
    """
    pass


class ObjectDatasetVersionStorage(VersionStorage, ObjectDatasetStorage):
    """
    This class
    """

    @abstractmethod
    def commit(self,
               dataset_name: str,
               dataset_version: ObjectDatasetVersion,
               amend: bool) -> ObjectDatasetVersion:
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
             dataset_version: ObjectDatasetVersion) -> ObjectDatasetVersion:
        """

        :param dataset_name:
        :param dataset_version:
        :return:
        """
        pass


class ObjectDatasetMetadataStorage(MetadataStorage, ObjectDatasetStorage):
    """
    This class
    """

    @abstractmethod
    def commit(self,
               dataset_name: str,
               dataset_version: ObjectDatasetVersion,
               dataset_metadata: ObjectDatasetMetadata,
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
             dataset_version: ObjectDatasetVersion) -> ObjectDatasetMetadata:
        """

        :param dataset_name:
        :param dataset_version:
        :return:
        """
        pass


class ObjectDatasetRecordStorage(RecordStorage, ObjectDatasetStorage):

    @abstractmethod
    def commit(self,
               dataset_name: str,
               dataset_version: ObjectDatasetVersion,
               dataset_record_data: ObjectDatasetRecordData,
               dataset_metadata: ObjectDatasetMetadata,
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
             dataset_version: ObjectDatasetVersion,
             dataset_metadata: ObjectDatasetMetadata,
             dataset_record_data: ObjectDatasetRecordData,
             working_directory: Path) -> ObjectDatasetRecordData:
        """

        :param dataset_record_data:
        :param dataset_name:
        :param dataset_version:
        :param dataset_metadata:
        :param working_directory:
        :return:
        """
        pass


class ObjectDatasetObjectStorage(ObjectStorage, ObjectDatasetStorage):

    @abstractmethod
    def commit(self,
               dataset_name: str,
               dataset_version: ObjectDatasetVersion,
               dataset_record_data: ObjectDatasetRecordData,
               dataset_metadata: ObjectDatasetMetadata,
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
             dataset_version: ObjectDatasetVersion,
             dataset_metadata: ObjectDatasetMetadata,
             dataset_record_data: ObjectDatasetRecordData,
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
