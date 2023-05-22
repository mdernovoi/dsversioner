"""
This module
"""

import hashlib
import json
import uuid
from pathlib import Path

from .Dataset import Dataset
from .ObjectDatasetMetadata import ObjectDatasetMetadata
from .ObjectDatasetRecordData import ObjectDatasetRecordData
from .ObjectDatasetStorage import ObjectDatasetVersionStorage, ObjectDatasetMetadataStorage, \
    ObjectDatasetRecordStorage, ObjectDatasetObjectStorage
from .ObjectDatasetVersion import ObjectDatasetVersion


class ObjectDataset(Dataset):
    """
    This class
    """

    def __init__(self,
                 name: str,
                 working_directory: Path,
                 version_storage: ObjectDatasetVersionStorage,
                 metadata_storage: ObjectDatasetMetadataStorage,
                 record_storage: ObjectDatasetRecordStorage,
                 object_storage: ObjectDatasetObjectStorage,
                 record_data: ObjectDatasetRecordData,
                 metadata: ObjectDatasetMetadata = None,
                 version: ObjectDatasetVersion = None,
                 ):
        self._name = name
        self._version = version
        if self._version is None:
            self._version = ObjectDatasetVersion()
        self._metadata = metadata
        if self._metadata is None:
            self._metadata = ObjectDatasetMetadata()

        self._record_data = record_data

        self._working_directory = working_directory

        self._object_storage = object_storage
        self._version_storage = version_storage
        self._metadata_storage = metadata_storage
        self._record_storage = record_storage

    @property
    def record_data(self) -> ObjectDatasetRecordData:
        """

        :return:
        """
        return self._record_data

    @property
    def working_directory(self) -> Path:
        """

        :return:
        """
        return self._working_directory

    @property
    def name(self) -> str:
        """

        :return:
        """
        return self._name

    @property
    def version(self) -> ObjectDatasetVersion:
        """

        :return:
        """
        return self._version

    @property
    def metadata(self) -> ObjectDatasetMetadata:
        """

        :return:
        """
        return self._metadata

    def init(self,
             index_dimension_name: str = "id",
             uri_dimension_name: str = "uri"
             ) -> None:

        self._metadata.private_metadata.index_dimension_name = index_dimension_name
        self._metadata.private_metadata.uri_dimension_name = uri_dimension_name

        self._version_storage.init(dataset_name=self.name)
        self._metadata_storage.init(dataset_name=self.name)
        self._object_storage.init(dataset_name=self.name)
        self._record_storage.init(dataset_name=self.name)

    def add(self,
            record_data: ObjectDatasetRecordData = None) -> None:
        """

        :param record_data:
        :return:
        """
        self._record_data = record_data

    def commit(self,
               version: ObjectDatasetVersion = None,
               amend: bool = False) -> ObjectDatasetVersion:
        """

        :param version:
        :param amend:
        :return:
        """

        if version is None:
            seed = str(uuid.uuid4())
            hash_object = hashlib.sha512(bytes(seed, 'utf-8'))
            version_name = hash_object.hexdigest()[0:10]
            version = ObjectDatasetVersion(name=version_name)

        committed_version = self._version_storage.commit(dataset_name=self.name,
                                                         dataset_version=version,
                                                         amend=amend)

        self._record_storage.commit(dataset_name=self.name,
                                    dataset_version=committed_version,
                                    dataset_record_data=self.record_data,
                                    dataset_metadata=self.metadata,
                                    working_directory=self._working_directory)

        self._object_storage.commit(dataset_name=self.name,
                                    dataset_version=committed_version,
                                    dataset_record_data=self.record_data,
                                    dataset_metadata=self.metadata,
                                    working_directory=self._working_directory)

        self._metadata_storage.commit(dataset_name=self.name,
                                      dataset_version=committed_version,
                                      dataset_metadata=self._metadata,
                                      amend=amend)

        self._version = committed_version
        return committed_version

    def pull(self,
             version: ObjectDatasetVersion = None) -> None:
        """

        :param version:
        :return:
        """

        pulled_version = self._version_storage.pull(dataset_name=self.name,
                                                    dataset_version=version)

        pulled_metadata = self._metadata_storage.pull(dataset_name=self.name,
                                                      dataset_version=pulled_version)

        pulled_records = self._record_storage.pull(
            dataset_name=self.name,
            dataset_version=pulled_version,
            dataset_metadata=pulled_metadata,
            dataset_record_data=self._record_data,
            working_directory=self._working_directory
        )

        self._object_storage.pull(
            dataset_name=self.name,
            dataset_version=pulled_version,
            dataset_metadata=pulled_metadata,
            dataset_record_data=pulled_records,
            working_directory=self._working_directory
        )

        self._metadata = pulled_metadata
        self._record_data = pulled_records
        self._version = pulled_version

    def drop(self) -> None:
        """

        :return:
        """

        self._version_storage.drop(dataset_name=self.name)
        self._metadata_storage.drop(dataset_name=self.name)
        self._object_storage.drop(dataset_name=self.name)
        self._record_storage.drop(dataset_name=self.name)

    def __str__(self):
        data = {
            'name': self._name,
            'version': self._version.to_json(),
            'metadata': self._metadata.to_json()
        }

        return json.dumps(data,
                          indent=4,
                          default=lambda obj: obj.to_json())

    def __repr__(self):
        return self.__str__()

