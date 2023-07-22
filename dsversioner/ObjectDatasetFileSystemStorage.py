"""
This module
"""

import json
import os
import shutil
from pathlib import Path

from .Storage import RecordStorageFormats
from .Exceptions import DatasetExistsException, InvalidRecordStorageFormatException, \
    DatasetVersionDoesNotExistException
from .FileSystemStorage import FileSystemStorage
from .ObjectDatasetMetadata import ObjectDatasetMetadata
from .ObjectDatasetStorage import ObjectDatasetVersionStorage, ObjectDatasetMetadataStorage, \
    ObjectDatasetRecordStorage, ObjectDatasetObjectStorage, ObjectDatasetRecordData
from .ObjectDatasetVersion import ObjectDatasetVersion
from .Serializable import JsonSerializable


class FileSystemObjectDatasetVersionStorageSchema(JsonSerializable):
    """
    This class
    """

    def __init__(self,
                 #versions: list[ObjectDatasetVersion] = None):
                 versions: list = None):
        self._versions = versions

    @property
    def versions(self) -> list: #list[ObjectDatasetVersion]:
        """

        :return:
        """
        return self._versions

    def to_json(self) -> dict:
        """

        :return:
        """
        to_return = {
            "versions": self._versions
        }
        return to_return

    @classmethod
    def from_json(cls, json_dict: dict):  # TODO: uncomment after migration to python 3.11 -> Self:
        """

        :param json_dict:
        :return:
        """
        if json_dict is None:
            return FileSystemObjectDatasetVersionStorageSchema()

        versions = [
            ObjectDatasetVersion.from_json(version) for
            version in json_dict['versions']]
        return FileSystemObjectDatasetVersionStorageSchema(versions=versions)


class FileSystemObjectDatasetVersionStorage(FileSystemStorage, ObjectDatasetVersionStorage):
    """
    This class
    """
    file_name = "version.json"

    def __init__(self,
                 root_path: Path):
        self._root_path = root_path

    @property
    def root_path(self):
        """

        :return:
        """
        return self._root_path

    def init(self,
             dataset_name: str) -> None:

        """

        :param dataset_name:
        :return:
        """

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            raise DatasetExistsException(dataset_name=dataset_name)

        os.makedirs(storage_path)

        # initial versions schema
        storage_data = FileSystemObjectDatasetVersionStorageSchema(
            versions=[]
        )
        with open(Path(storage_path, self.file_name), "w") as f:
            f.write(json.dumps(storage_data,
                               default=lambda obj: obj.to_json(),
                               indent=4))

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

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier, self.file_name)

        # get data from version storage
        with open(storage_path, "r") as f:
            content = f.read()
            storage_data = FileSystemObjectDatasetVersionStorageSchema.from_json(json.loads(content))

        # get new version id
        version_ids = [version.id for version in storage_data.versions]
        max_version_id = max(version_ids) if len(version_ids) != 0 else 0

        committed_version = None
        if amend:
            for index, version in enumerate(storage_data.versions):
                if version.id == max_version_id:
                    # replace last version
                    committed_version = ObjectDatasetVersion(
                        name=dataset_version.name,
                        id=version.id
                    )

                    storage_data.versions[index] = committed_version
        else:
            committed_version = ObjectDatasetVersion(
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
             dataset_version: ObjectDatasetVersion = None) -> ObjectDatasetVersion:

        """

        :param dataset_name:
        :param dataset_version:
        :return:
        """

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier, self.file_name)

        # get data from version storage
        with open(storage_path, "r") as f:
            content = f.read()
            storage_data = FileSystemObjectDatasetVersionStorageSchema.from_json(json.loads(content))

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
                                                  dataset_version=f"id: {dataset_version.id}")

    def drop(self,
             dataset_name: str) -> None:

        """

        :param dataset_name:
        :return:
        """

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            shutil.rmtree(storage_path)


class FileSystemObjectDatasetMetadataStorageContainerSchema(JsonSerializable):
    """
    This class
    """

    def __init__(self,
                 version_id: int = None,
                 metadata: ObjectDatasetMetadata = None):
        self._version_id = version_id
        self._metadata = metadata

    @property
    def version_id(self) -> int:
        """

        :return:
        """
        return self._version_id

    @property
    def metadata(self) -> ObjectDatasetMetadata:
        """

        :return:
        """
        return self._metadata

    def to_json(self) -> dict:
        """

        :return:
        """
        to_return = {
            "version_id": self._version_id,
            "metadata": self._metadata
        }
        return to_return

    @classmethod
    def from_json(cls, json_dict: dict):  # TODO: uncomment after migration to python 3.11 -> Self:

        """

        :param json_dict:
        :return:
        """
        if json_dict is None:
            return FileSystemObjectDatasetMetadataStorageContainerSchema()

        version_id = json_dict["version_id"]
        metadata = ObjectDatasetMetadata.from_json(json_dict["metadata"])
        return FileSystemObjectDatasetMetadataStorageContainerSchema(version_id=version_id, metadata=metadata)


class FileSystemObjectDatasetMetadataStorageSchema(JsonSerializable):
    """
    This class...
    """

    def __init__(self,
                 #metadata: list[FileSystemObjectDatasetMetadataStorageContainerSchema] = None):
                 metadata: list= None):
        self._metadata = metadata

    @property
    def metadata(self) -> list: #list[FileSystemObjectDatasetMetadataStorageContainerSchema]:
        """

        :return:
        """
        return self._metadata

    def to_json(self) -> dict:
        """

        :return:
        """
        to_return = {
            "metadata": self._metadata
        }
        return to_return

    @classmethod
    def from_json(cls, json_dict: dict):  # TODO: uncomment after migration to python 3.11 -> Self:
        """

        :param json_dict:
        :return:
        """

        if json_dict is None:
            return FileSystemObjectDatasetMetadataStorageSchema()

        metadata = [
            FileSystemObjectDatasetMetadataStorageContainerSchema.from_json(metadata_container) for
            metadata_container in json_dict['metadata']]
        return FileSystemObjectDatasetMetadataStorageSchema(metadata=metadata)


class FileSystemObjectDatasetMetadataStorage(FileSystemStorage, ObjectDatasetMetadataStorage):
    """
    This class
    """

    file_name = "metadata.json"

    def __init__(self,
                 root_path: Path):
        self._root_path = root_path

    @property
    def root_path(self) -> Path:
        """

        :return:
        """
        return self._root_path

    def init(self,
             dataset_name: str) -> None:
        """

        :param dataset_name:
        :return:
        """

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            raise DatasetExistsException(dataset_name=dataset_name)

        os.makedirs(storage_path)

        # initial metadata schema
        storage_data = FileSystemObjectDatasetMetadataStorageSchema(
            metadata=[]
        )
        with open(Path(storage_path, self.file_name), "w") as f:
            f.write(json.dumps(storage_data,
                               default=lambda obj: obj.to_json(),
                               indent=4))

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
             dataset_version: ObjectDatasetVersion) -> ObjectDatasetMetadata:

        """

        :param dataset_name:
        :param dataset_version:
        :return:
        """

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
                                                  dataset_version=f"id: {dataset_version.id}")

    def drop(self, dataset_name: str) -> None:
        """

        :param dataset_name:
        :return:
        """

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            shutil.rmtree(storage_path)


class FileSystemObjectDatasetRecordStorage(FileSystemStorage, ObjectDatasetRecordStorage):
    """
    This class
    """

    def __init__(self,
                 root_path: Path,
                 storage_format: RecordStorageFormats = RecordStorageFormats.CSV):

        self._root_path = root_path
        self._storage_format = storage_format

    @property
    def root_path(self) -> Path:
        """

        :return:
        """
        return self._root_path

    def init(self,
             dataset_name: str) -> None:
        """

        :param dataset_name:
        :return:
        """

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            raise DatasetExistsException(dataset_name=dataset_name)

        os.makedirs(storage_path)

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

        file_name = f"{dataset_name}_{str(dataset_version.id)}"
        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if self._storage_format is RecordStorageFormats.CSV:
            file_extension = ".csv"
            dataset_record_data.to_csv(
                path=Path(storage_path, f"{file_name}{file_extension}"),
                write_header=True,
                write_index_dimension=True,
                index_dimension_name=dataset_metadata.private_metadata.index_dimension_name
            )
        elif self._storage_format is RecordStorageFormats.PARQUET:
            file_extension = ".parquet"
            dataset_record_data.to_parquet(
                path=Path(storage_path, f"{file_name}{file_extension}")
            )
        else:
            raise InvalidRecordStorageFormatException(dataset_name=dataset_name)

        dataset_metadata.private_metadata.record_storage_data_location = f"{file_name}{file_extension}"

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

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier,
                            dataset_metadata.private_metadata.record_storage_data_location)

        if self._storage_format is RecordStorageFormats.CSV:
            to_return = dataset_record_data.from_csv(
                path=storage_path,
                # use first row as header
                header_rows=0,
                index_dimension_name=dataset_metadata.private_metadata.index_dimension_name
            )

            return to_return

        elif self._storage_format is RecordStorageFormats.PARQUET:
            to_return = dataset_record_data.from_parquet(
                path=storage_path
            )

            return to_return

        # did not find anything
        raise DatasetVersionDoesNotExistException(dataset_name=dataset_name,
                                                  dataset_version=f"id: {dataset_version.id}")

    def drop(self, dataset_name: str) -> None:
        """

        :param dataset_name:
        :return:
        """
        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            shutil.rmtree(storage_path)


class FileSystemObjectDatasetObjectStorage(FileSystemStorage, ObjectDatasetObjectStorage):
    """
    This class
    """

    def __init__(self,
                 root_path: Path):
        self._root_path = root_path

    @property
    def root_path(self) -> Path:
        """

        :return:
        """
        return self._root_path

    def init(self,
             dataset_name: str) -> None:
        """

        :param dataset_name:
        :return:
        """
        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            raise DatasetExistsException(dataset_name=dataset_name)

        os.makedirs(storage_path)

    def commit(self,
               dataset_name: str,
               dataset_version: ObjectDatasetVersion,
               dataset_record_data: ObjectDatasetRecordData,
               dataset_metadata: ObjectDatasetMetadata,
               working_directory: Path
               ) -> None:

        """

        :param dataset_name:
        :param dataset_version:
        :param dataset_record_data:
        :param dataset_metadata:
        :param working_directory:
        :return:
        """

        container_name = f"{dataset_name}_{str(dataset_version.id)}"
        container_storage_path = Path(self.root_path, dataset_name, self.storage_identifier, container_name)

        # clear and create storage path
        if os.path.exists(container_storage_path):
            shutil.rmtree(container_storage_path)
        os.makedirs(container_storage_path)

        object_locations = dataset_record_data.record_data[
            dataset_metadata.private_metadata.uri_dimension_name].values.tolist()
        object_locations = [Path(object_location) for object_location in object_locations]

        for object_location in object_locations:
            shutil.copyfile(Path(working_directory, object_location),
                            Path(container_storage_path, object_location.name))

        dataset_metadata.private_metadata.object_storage_data_location = container_name

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

        container_name = dataset_metadata.private_metadata.object_storage_data_location
        container_storage_path = Path(self.root_path, dataset_name, self.storage_identifier, container_name)

        # did not find anything
        if not os.path.exists(container_storage_path):
            raise DatasetVersionDoesNotExistException(dataset_name=dataset_name,
                                                      dataset_version=f"id:{dataset_version.id}")

        object_locations = dataset_record_data.record_data[
            dataset_metadata.private_metadata.uri_dimension_name].values.tolist()
        object_locations = [Path(object_location) for object_location in object_locations]

        for object_location in object_locations:
            shutil.copyfile(Path(container_storage_path, object_location.name),
                            Path(working_directory, object_location))

    def drop(self,
             dataset_name: str) -> None:
        """

        :param dataset_name:
        :return:
        """

        storage_path = Path(self.root_path, dataset_name, self.storage_identifier)

        if os.path.exists(storage_path):
            shutil.rmtree(storage_path)
