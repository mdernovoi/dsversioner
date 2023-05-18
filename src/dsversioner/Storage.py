import json
import os
import shutil
from abc import ABCMeta, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Tuple

from .DatasetMetadata import DatasetMetadata
from .DatasetRecordData import DatasetRecordData
from .DatasetVersion import DatasetVersion
from .Exceptions import DatasetExistsException, InvalidFileSystemStorageFormatException


class FileSystemStorage(metaclass=ABCMeta):

    class Format(Enum):
        CSV = 1
        PARQUET = 2

    @property
    @abstractmethod
    def root_path(self):
        pass


class VersionStorage(metaclass=ABCMeta):
    storage_identifier = "version"

    @abstractmethod
    def init(self,
             dataset_name: str):
        pass

    @abstractmethod
    def commit(self,
               dataset_name: str,
               dataset_version: DatasetVersion,
               dataset_metadata: DatasetMetadata,
               amend: bool) -> DatasetVersion:
        pass

    @abstractmethod
    def pull(self,
             dataset_version: DatasetVersion = None) -> DatasetVersion:
        pass

    @abstractmethod
    def drop(self,
             dataset_name: str):
        pass


class ObjectStorage(metaclass=ABCMeta):
    storage_identifier = "object"

    @abstractmethod
    def init(self,
             dataset_name: str):
        pass

    @abstractmethod
    def drop(self,
             dataset_name: str):
        pass


class RecordStorage(metaclass=ABCMeta):
    storage_identifier = "record"

    @abstractmethod
    def init(self,
             dataset_name: str):
        pass

    @abstractmethod
    def commit(self,
               dataset_name: str,
               dataset_record_data: DatasetRecordData,
               dataset_version: DatasetVersion,
               index_dimension_name: str) -> None:
        pass

    @abstractmethod
    def drop(self,
             dataset_name: str):
        pass


class FileSystemVersionStorageLineageContainerSchema:
    def __init__(self,
                 version: DatasetVersion,
                 metadata: DatasetMetadata):
        self._version = version
        self._metadata = metadata

    @property
    def version(self):
        return self._version

    @property
    def metadata(self):
        return self._metadata

    def to_json(self):
        to_return = {
            "version": self._version,
            "metadata": self._metadata
        }
        return to_return

    @classmethod
    def from_json(cls, json_dict):
        version = DatasetVersion.from_json(json_dict["version"])
        metadata = DatasetMetadata.from_json(json_dict["metadata"])
        return FileSystemVersionStorageLineageContainerSchema(version=version, metadata=metadata)


class FileSystemVersionStorageSchema:
    def __init__(self, lineage: list[FileSystemVersionStorageLineageContainerSchema]):
        self._lineage = lineage

    @property
    def lineage(self):
        return self._lineage

    def to_json(self):
        to_return = {
            "lineage": self._lineage
        }
        return to_return

    @classmethod
    def from_json(cls, json_dict):
        lineage = [
            FileSystemVersionStorageLineageContainerSchema.from_json(version_container) for
            version_container in json_dict['lineage']]
        return FileSystemVersionStorageSchema(lineage=lineage)


class FileSystemVersionStorage(FileSystemStorage, VersionStorage):
    file_name = "version.json"

    def __init__(self,
                 root_path: Path):
        self._root_path = root_path

    @property
    def root_path(self):
        return self._root_path

    def init(self, dataset_name: str):
        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            raise DatasetExistsException(dataset_name=dataset_name)

        os.makedirs(storage_path)

        # initial lineage schema
        storage_path = Path(self.root_path, dataset_name, self.storage_identifier, self.file_name)
        storage_data = FileSystemVersionStorageSchema(
            lineage=[]
        )
        with open(storage_path, "w") as f:
            f.write(json.dumps(storage_data,
                               default=lambda obj: obj.to_json(),
                               indent=4))

    def commit(self,
               dataset_name: str,
               dataset_version: DatasetVersion,
               dataset_metadata: DatasetMetadata,
               amend: bool) -> DatasetVersion:

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier, self.file_name)

        # get data from version storage
        with open(storage_path, "r") as f:
            content = f.read()
            storage_data = FileSystemVersionStorageSchema.from_json(json.loads(content))

        # get new version id
        version_ids = [version_container.version.id for version_container in storage_data.lineage]
        max_version_id = max(version_ids) if len(version_ids) != 0 else 0

        committed_version = None
        if amend:
            for index, version_container in enumerate(storage_data.lineage):
                if version_container.version.id == max_version_id:
                    # replace last version
                    committed_version = DatasetVersion(
                            name=dataset_version.name,
                            id=version_container.version.id
                        )
                    new_version_container = FileSystemVersionStorageLineageContainerSchema(
                        version=committed_version,
                        metadata=dataset_metadata
                    )

                    storage_data.lineage[index] = new_version_container
        else:
            committed_version = DatasetVersion(
                    name=dataset_version.name,
                    id=max_version_id + 1
                )
            new_version_container = FileSystemVersionStorageLineageContainerSchema(
                version=committed_version,
                metadata=dataset_metadata
            )

            storage_data.lineage.append(new_version_container)

        # write new version to version storage
        with open(storage_path, "w") as f:
            f.write(json.dumps(storage_data,
                               default=lambda obj: obj.to_json(),
                               indent=4))

        return committed_version

    def drop(self, dataset_name: str):
        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            shutil.rmtree(storage_path)


class FileSystemRecordStorage(FileSystemStorage, RecordStorage):

    def __init__(self,
                 root_path: Path,
                 storage_format: FileSystemStorage.Format = FileSystemStorage.Format.CSV):
        self._root_path = root_path
        self._storage_format = storage_format

    @property
    def root_path(self):
        return self._root_path

    def init(self, dataset_name: str):
        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            raise DatasetExistsException(dataset_name=dataset_name)

        os.makedirs(storage_path)

    def commit(self, dataset_name: str,
               dataset_record_data: DatasetRecordData,
               dataset_version: DatasetVersion,
               index_dimension_name: str):

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier,
                            f"{dataset_name}_{str(dataset_version.id)}")

        if self._storage_format is FileSystemStorage.Format.CSV:
            dataset_record_data.to_csv(
                path=storage_path.with_suffix(".csv"),
                write_header=True,
                index_dimension_name=index_dimension_name
            )
        elif self._storage_format is FileSystemStorage.Format.PARQUET:
            dataset_record_data.to_parquet(
                path=storage_path.with_suffix(".parquet")
            )
        else:
            raise InvalidFileSystemStorageFormatException(dataset_name=dataset_name)

    def drop(self, dataset_name: str):
        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            shutil.rmtree(storage_path)


class FileSystemObjectStorage(FileSystemStorage, ObjectStorage):

    def __init__(self,
                 root_path: Path):
        self._root_path = root_path

    @property
    def root_path(self):
        return self._root_path

    def init(self, dataset_name: str):
        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            raise DatasetExistsException(dataset_name=dataset_name)

        os.makedirs(storage_path)

    def drop(self, dataset_name: str):
        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            shutil.rmtree(storage_path)

# class S3Storage(Storage):
#     pass
#
#
# class MinioStorage(S3Storage):
#     pass
