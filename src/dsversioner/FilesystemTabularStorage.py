import json
import os
import shutil
from collections import namedtuple
from pathlib import Path

import pandas

from .Exceptions import *
from .Storage import TabularStorage
from .DatasetVersion import DatasetVersion
from .DatasetMetadata import DatasetMetadata


class FileSystemTabularStorageDatasetMetadata(DatasetMetadata):
    def __init__(self,
                 private_metadata: dict,
                 public_metadata: dict):
        super().__init__(
            private_metadata=private_metadata,
            public_metadata=public_metadata
        )

    def to_json(self):
        to_return = {
            "private_metadata": super().private_metadata,
            "public_metadata": super().public_metadata
        }
        return to_return

    @classmethod
    def from_json(cls, json_dict):
        return FileSystemTabularStorageDatasetMetadata(private_metadata=json_dict['private_metadata'],
                                                       public_metadata=json_dict['public_metadata'])


class FileSystemTabularStorageDatasetVersion(DatasetVersion):
    def __init__(self,
                 id: int,
                 name: str,
                 dataset_metadata: FileSystemTabularStorageDatasetMetadata = None
                 ):
        self._dataset_metadata = dataset_metadata
        super().__init__(id=id,
                         name=name)

    @property
    def dataset_metadata(self):
        return self._dataset_metadata

    @dataset_metadata.setter
    def dataset_metadata(self, dataset_metadata):
        self._dataset_metadata = dataset_metadata

    def to_json(self):
        to_return = {
            "version_id": super().version_id,
            "version_name": super().version_name,
            "dataset_metadata": self.dataset_metadata
        }
        return to_return

    @classmethod
    def from_json(cls, json_dict):
        return FileSystemTabularStorageDatasetVersion(id=json_dict['version_id'],
                                                      name=json_dict['version_name'],
                                                      dataset_metadata=FileSystemTabularStorageDatasetMetadata.from_json(
                                                          json_dict['dataset_metadata']))


class FileSystemTabularStorageSchema:
    def __init__(self,
                 versions: list[FileSystemTabularStorageDatasetVersion]):
        self._versions = versions

    @property
    def versions(self):
        return self._versions

    @versions.setter
    def versions(self, versions):
        self._versions = versions

    @classmethod
    def from_json(cls, json_dict):
        versions = [FileSystemTabularStorageDatasetVersion.from_json(version) for version in json_dict['versions']]
        return FileSystemTabularStorageSchema(versions=versions)

    def to_json(self):
        to_return = {
            "versions": self.versions
        }
        return to_return


class FileSystemTabularStorage(TabularStorage):

    def __init__(self,
                 path: str,
                 versioning_information_file_name: str = "DATASET_VERSIONS.json"):
        self._datasets_root_path = path
        self._versioning_information_file_name = versioning_information_file_name

    def dataset_init(self,
                     dataset_name: str) -> None:

        if os.path.exists(Path(self._datasets_root_path, dataset_name)):
            raise DatasetExistsException()

        os.makedirs(Path(self._datasets_root_path, dataset_name))
        with open(Path(self._datasets_root_path, dataset_name, self._versioning_information_file_name), "w") as f:
            # bla = json.dumps(FileSystemTabularStorageSchema(versions=[
            #     FileSystemTabularStorageDatasetVersion(5, "dd", dataset_metadata=
            #     FileSystemTabularStorageDatasetMetadata(
            #         private_metadata={"aa": 7},
            #         public_metadata={"hh": True}
            #     ))
            # ]),
            #     default=lambda obj: obj.to_json(),
            #     indent=4)
            # f.write(bla)
            f.write(json.dumps(FileSystemTabularStorageSchema(versions=list()),
                               default=lambda obj: obj.to_json(),
                               indent=4))

    def dataset_commit(self,
                       dataset_name: str,
                       data_frame: pandas.DataFrame,
                       index_dimension_name: str,
                       uri_dimension_name: str,
                       public_metadata: dict,
                       private_metadata: dict,
                       version_name: str,
                       overwrite_last_commit: bool):

        with open(Path(self._datasets_root_path, dataset_name, self._versioning_information_file_name), "r") as f:
            content = f.read()
            storage_data = FileSystemTabularStorageSchema.from_json(json.loads(content))

        version_ids = [version.version_id for version in storage_data.versions]
        version_names = [version.version_name for version in storage_data.versions]

        if len(version_ids) == 0:
            max_version_id = 0
            last_version = None
        else:
            max_version_id = max(version_ids)
            last_version = [version for version in storage_data.versions if version.version_id == max_version_id][0]

        new_version_id = max_version_id + 1

        if version_name in version_names and overwrite_last_commit is False:
            raise DatasetVersionExistsException

        if last_version is not None and version_name != last_version.version_name and overwrite_last_commit is True:
            raise DatasetVersionExistsException

        dataset_version_file_name = f"{dataset_name}_{version_name}.csv"
        data_frame.to_csv(
            path_or_buf=Path(self._datasets_root_path, dataset_name, dataset_version_file_name),
            header=True,
            index=True,
            index_label=index_dimension_name
        )

        new_dataset_version = FileSystemTabularStorageDatasetVersion(
            id=new_version_id,
            name=version_name,
            dataset_metadata=FileSystemTabularStorageDatasetMetadata(
                private_metadata=private_metadata,
                public_metadata=public_metadata
            )
        )

        storage_data.versions.append(new_dataset_version)

        with open(Path(self._datasets_root_path, dataset_name, self._versioning_information_file_name), "w") as f:
            f.write(json.dumps(storage_data,
                    default=lambda obj: obj.to_json(),
                    indent=4)
                    )

    def dataset_drop(self,
                     dataset_name: str) -> None:

        if os.path.exists(Path(self._datasets_root_path, dataset_name)):
            shutil.rmtree(Path(self._datasets_root_path, dataset_name))
