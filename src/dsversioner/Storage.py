import json
import os
import shutil
from abc import ABCMeta, abstractmethod
from enum import Enum
from pathlib import Path

from .DatasetMetadata import DatasetMetadata, ObjectDatasetMetadata
from .DatasetRecordData import DatasetRecordData
from .DatasetVersion import DatasetVersion
from .Exceptions import DatasetExistsException, InvalidFileSystemStorageFormatException, \
    DatasetVersionDoesNotExistException


class FileSystemStorage(metaclass=ABCMeta):
    class Format(Enum):
        CSV = 1
        PARQUET = 2

    @property
    @abstractmethod
    def root_path(self):
        pass


class ObjectDatasetStorage(metaclass=ABCMeta):
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
               amend: bool) -> DatasetVersion:
        pass

    @abstractmethod
    def pull(self,
             dataset_name: str,
             dataset_version: DatasetVersion = None) -> DatasetVersion:
        pass

    @abstractmethod
    def drop(self,
             dataset_name: str):
        pass


class MetadataStorage(metaclass=ABCMeta):
    storage_identifier = "metadata"

    @abstractmethod
    def init(self,
             dataset_name: str):
        pass

    @abstractmethod
    def commit(self,
               dataset_name: str,
               dataset_version: DatasetVersion,
               dataset_metadata: DatasetMetadata,
               amend: bool) -> None:
        pass

    @abstractmethod
    def pull(self,
             dataset_name: str,
             dataset_version: DatasetVersion) -> DatasetMetadata:
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
               dataset_version: DatasetVersion,
               dataset_record_data: DatasetRecordData,
               dataset_metadata: DatasetMetadata) -> None:
        pass

    @abstractmethod
    def pull(self,
             dataset_name: str,
             dataset_version: DatasetVersion,
             dataset_metadata: DatasetMetadata) -> DatasetRecordData:
        pass

    @abstractmethod
    def drop(self,
             dataset_name: str):
        pass


class ObjectDatasetMetadataStorage(MetadataStorage, ObjectDatasetStorage):

    @abstractmethod
    def commit(self,
               dataset_name: str,
               dataset_version: DatasetVersion,
               dataset_metadata: ObjectDatasetMetadata,
               amend: bool) -> None:
        pass

    @abstractmethod
    def pull(self,
             dataset_name: str,
             dataset_version: DatasetVersion) -> ObjectDatasetMetadata:
        pass


class ObjectDatasetRecordStorage(RecordStorage, ObjectDatasetStorage):

    @abstractmethod
    def commit(self,
               dataset_name: str,
               dataset_version: DatasetVersion,
               dataset_record_data: DatasetRecordData,
               dataset_metadata: ObjectDatasetMetadata) -> None:
        pass

    @abstractmethod
    def pull(self,
             dataset_name: str,
             dataset_version: DatasetVersion,
             dataset_metadata: ObjectDatasetMetadata) -> DatasetRecordData:
        pass


# class FileSystemVersionStorageLineageContainerSchema:
#     def __init__(self,
#                  version: DatasetVersion,
#                  metadata: DatasetMetadata):
#         self._version = version
#         self._metadata = metadata
#
#     @property
#     def version(self):
#         return self._version
#
#     @property
#     def metadata(self):
#         return self._metadata
#
#     def to_json(self):
#         to_return = {
#             "version": self._version,
#             "metadata": self._metadata
#         }
#         return to_return
#
#     @classmethod
#     def from_json(cls, json_dict):
#         version = DatasetVersion.from_json(json_dict["version"])
#         metadata = DatasetMetadata.from_json(json_dict["metadata"])
#         return FileSystemVersionStorageLineageContainerSchema(version=version, metadata=metadata)


class FileSystemVersionStorageSchema:
    def __init__(self, versions: list[DatasetVersion]):
        self._versions = versions

    @property
    def versions(self):
        return self._versions

    def to_json(self):
        to_return = {
            "versions": self._versions
        }
        return to_return

    @classmethod
    def from_json(cls, json_dict):
        versions = [
            DatasetVersion.from_json(version) for
            version in json_dict['versions']]
        return FileSystemVersionStorageSchema(versions=versions)


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

        # initial versions schema
        storage_path = Path(self.root_path, dataset_name, self.storage_identifier, self.file_name)
        storage_data = FileSystemVersionStorageSchema(
            versions=[]
        )
        with open(storage_path, "w") as f:
            f.write(json.dumps(storage_data,
                               default=lambda obj: obj.to_json(),
                               indent=4))

    def commit(self,
               dataset_name: str,
               dataset_version: DatasetVersion,
               amend: bool) -> DatasetVersion:

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier, self.file_name)

        # get data from version storage
        with open(storage_path, "r") as f:
            content = f.read()
            storage_data = FileSystemVersionStorageSchema.from_json(json.loads(content))

        # get new version id
        version_ids = [version.id for version in storage_data.versions]
        max_version_id = max(version_ids) if len(version_ids) != 0 else 0

        committed_version = None
        if amend:
            for index, version in enumerate(storage_data.versions):
                if version.id == max_version_id:
                    # replace last version
                    committed_version = DatasetVersion(
                        name=dataset_version.name,
                        id=version.id
                    )

                    storage_data.versions[index] = committed_version
        else:
            committed_version = DatasetVersion(
                name=dataset_version.name,
                id=max_version_id + 1
            )

            storage_data.versions.append(committed_version)

        # write new version to version storage
        with open(storage_path, "w") as f:
            f.write(json.dumps(storage_data,
                               default=lambda obj: obj.to_json(),
                               indent=4))

        return committed_version

    def pull(self,
             dataset_name: str,
             dataset_version: DatasetVersion = None) -> DatasetVersion:

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier, self.file_name)

        # get data from version storage
        with open(storage_path, "r") as f:
            content = f.read()
            storage_data = FileSystemVersionStorageSchema.from_json(json.loads(content))

        version_ids = [version.id for version in storage_data.versions]
        max_version_id = max(version_ids) if len(version_ids) != 0 else 0

        if dataset_version is None:
            # return latest
            for index, version in enumerate(storage_data.versions):
                if version.id == max_version_id:
                    return version
        else:
            # search for version with index
            for index, version in enumerate(storage_data.versions):
                if version.id == dataset_version.id:
                    return version

        # did not find anything
        raise DatasetVersionDoesNotExistException(dataset_name=dataset_name,
                                                  dataset_version_name=dataset_version.id)

    def drop(self, dataset_name: str):
        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            shutil.rmtree(storage_path)


class FileSystemObjectDatasetMetadataStorageContainerSchema:
    def __init__(self,
                 version_id: int,
                 metadata: ObjectDatasetMetadata):
        self._version_id = version_id
        self._metadata = metadata

    @property
    def version_id(self):
        return self._version_id

    @property
    def metadata(self):
        return self._metadata

    def to_json(self):
        to_return = {
            "version_id": self._version_id,
            "metadata": self._metadata
        }
        return to_return

    @classmethod
    def from_json(cls, json_dict):
        version_id = json_dict["version_id"]
        metadata = ObjectDatasetMetadata.from_json(json_dict["metadata"])
        return FileSystemObjectDatasetMetadataStorageContainerSchema(version_id=version_id, metadata=metadata)


class FileSystemObjectDatasetMetadataStorageSchema:
    def __init__(self, metadata: list[FileSystemObjectDatasetMetadataStorageContainerSchema]):
        self._metadata = metadata

    @property
    def metadata(self):
        return self._metadata

    def to_json(self):
        to_return = {
            "metadata": self._metadata
        }
        return to_return

    @classmethod
    def from_json(cls, json_dict):
        metadata = [
            FileSystemObjectDatasetMetadataStorageContainerSchema.from_json(version) for
            version in json_dict['metadata']]
        return FileSystemObjectDatasetMetadataStorageSchema(metadata=metadata)


class FileSystemObjectDatasetMetadataStorage(FileSystemStorage, ObjectDatasetMetadataStorage):
    file_name = "metadata.json"

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

        # initial metadata schema
        storage_path = Path(self.root_path, dataset_name, self.storage_identifier, self.file_name)
        storage_data = FileSystemObjectDatasetMetadataStorageSchema(
            metadata=[]
        )
        with open(storage_path, "w") as f:
            f.write(json.dumps(storage_data,
                               default=lambda obj: obj.to_json(),
                               indent=4))

    def commit(self,
               dataset_name: str,
               dataset_version: DatasetVersion,
               dataset_metadata: ObjectDatasetMetadata,
               amend: bool) -> None:

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier, self.file_name)

        # get data from metadata storage
        with open(storage_path, "r") as f:
            content = f.read()
            storage_data = FileSystemObjectDatasetMetadataStorageSchema.from_json(json.loads(content))

        new_metadata_container = FileSystemObjectDatasetMetadataStorageContainerSchema(
            version_id=dataset_version.id,
            metadata=dataset_metadata
        )

        if amend:
            for index, metadata_container in enumerate(storage_data.metadata):
                if metadata_container.version_id == dataset_version.id:
                    # replace
                    storage_data.metadata[index] = new_metadata_container
        else:
            storage_data.metadata.append(new_metadata_container)

        # write new version to version storage
        with open(storage_path, "w") as f:
            f.write(json.dumps(storage_data,
                               default=lambda obj: obj.to_json(),
                               indent=4))

    def pull(self,
             dataset_name: str,
             dataset_version: DatasetVersion) -> ObjectDatasetMetadata:

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier, self.file_name)

        # get data from version storage
        with open(storage_path, "r") as f:
            content = f.read()
            storage_data = FileSystemObjectDatasetMetadataStorageSchema.from_json(json.loads(content))

        for index, metadata_container in enumerate(storage_data.metadata):
            if metadata_container.version_id == dataset_version.id:
                return metadata_container.metadata

        # did not find anything
        raise DatasetVersionDoesNotExistException(dataset_name=dataset_name,
                                                  dataset_version_name=dataset_version.id)

    def drop(self, dataset_name: str):
        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            shutil.rmtree(storage_path)


# class FileSystemVersionStorage(FileSystemStorage, VersionStorage):
#     file_name = "version.json"
#
#     def __init__(self,
#                  root_path: Path):
#         self._root_path = root_path
#
#     @property
#     def root_path(self):
#         return self._root_path
#
#     def init(self, dataset_name: str):
#         storage_path = Path(self.root_path, dataset_name, self.storage_identifier)
#
#         if os.path.exists(storage_path):
#             raise DatasetExistsException(dataset_name=dataset_name)
#
#         os.makedirs(storage_path)
#
#         # initial lineage schema
#         storage_path = Path(self.root_path, dataset_name, self.storage_identifier, self.file_name)
#         storage_data = FileSystemVersionStorageSchema(
#             lineage=[]
#         )
#         with open(storage_path, "w") as f:
#             f.write(json.dumps(storage_data,
#                                default=lambda obj: obj.to_json(),
#                                indent=4))
#
#     def commit(self,
#                dataset_name: str,
#                dataset_version: DatasetVersion,
#                dataset_metadata: DatasetMetadata,
#                amend: bool) -> DatasetVersion:
#
#         storage_path = Path(self.root_path, dataset_name, self.storage_identifier, self.file_name)
#
#         # get data from version storage
#         with open(storage_path, "r") as f:
#             content = f.read()
#             storage_data = FileSystemVersionStorageSchema.from_json(json.loads(content))
#
#         # get new version id
#         version_ids = [version_container.version.id for version_container in storage_data.lineage]
#         max_version_id = max(version_ids) if len(version_ids) != 0 else 0
#
#         committed_version = None
#         if amend:
#             for index, version_container in enumerate(storage_data.lineage):
#                 if version_container.version.id == max_version_id:
#                     # replace last version
#                     committed_version = DatasetVersion(
#                         name=dataset_version.name,
#                         id=version_container.version.id
#                     )
#                     new_version_container = FileSystemVersionStorageLineageContainerSchema(
#                         version=committed_version,
#                         metadata=dataset_metadata
#                     )
#
#                     storage_data.lineage[index] = new_version_container
#         else:
#             committed_version = DatasetVersion(
#                 name=dataset_version.name,
#                 id=max_version_id + 1
#             )
#             new_version_container = FileSystemVersionStorageLineageContainerSchema(
#                 version=committed_version,
#                 metadata=dataset_metadata
#             )
#
#             storage_data.lineage.append(new_version_container)
#
#         # write new version to version storage
#         with open(storage_path, "w") as f:
#             f.write(json.dumps(storage_data,
#                                default=lambda obj: obj.to_json(),
#                                indent=4))
#
#         return committed_version
#
#     def drop(self, dataset_name: str):
#         storage_path = Path(self.root_path, dataset_name, self.storage_identifier)
#
#         if os.path.exists(storage_path):
#             shutil.rmtree(storage_path)

class FileSystemObjectDatasetRecordStorage(FileSystemStorage, ObjectDatasetRecordStorage):

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

    def commit(self, dataset_name: str, dataset_version: DatasetVersion,
               dataset_record_data: DatasetRecordData,
               dataset_metadata: ObjectDatasetMetadata) -> None:

        file_name = f"{dataset_name}_{str(dataset_version.id)}"
        storage_path = Path(self.root_path, dataset_name, self.storage_identifier, file_name)

        if self._storage_format is FileSystemStorage.Format.CSV:
            dataset_record_data.to_csv(
                path=storage_path.with_suffix(".csv"),
                header=True,
                index_dimension_name=dataset_metadata.private_metadata.index_dimension_name
            )
            file_name = f"{file_name}.csv"
        elif self._storage_format is FileSystemStorage.Format.PARQUET:
            dataset_record_data.to_parquet(
                path=storage_path.with_suffix(".parquet")
            )
            file_name = f"{file_name}.parquet"
        else:
            raise InvalidFileSystemStorageFormatException(dataset_name=dataset_name)

        dataset_metadata.private_metadata.record_storage_data_location = file_name

    def pull(self,
             dataset_name: str,
             dataset_version: DatasetVersion,
             dataset_metadata: ObjectDatasetMetadata) -> DatasetRecordData:

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier,
                            dataset_metadata.private_metadata.record_storage_data_location)

        if self._storage_format is FileSystemStorage.Format.CSV:
            return DatasetRecordData.from_csv(
                path=storage_path,
                # use first row as header
                header=0,
                index_dimension_name=dataset_metadata.private_metadata.index_dimension_name
            )
        elif self._storage_format is FileSystemStorage.Format.PARQUET:
            return DatasetRecordData.from_parquet(
                path=storage_path
            )

        # did not find anything
        raise DatasetVersionDoesNotExistException(dataset_name=dataset_name,
                                                  dataset_version_name=dataset_version.id)

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
