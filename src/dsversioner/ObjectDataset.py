import hashlib
import uuid

import pandas

from .Dataset import Dataset
from .DatasetMetadata import DatasetMetadata
from .DatasetVersion import DatasetVersion
from .Storage import VersionStorage, ObjectStorage, RecordStorage
from .DatasetRecordData import DatasetRecordData


class ObjectDataset(Dataset):

    def __init__(self,
                 name: str,
                 object_storage: ObjectStorage,
                 version_storage: VersionStorage,
                 record_storage: RecordStorage,
                 metadata: DatasetMetadata = None,
                 version: DatasetVersion = None,
                 record_data: DatasetRecordData = None
                 ):
        self._object_storage = object_storage
        self._version_storage = version_storage
        self._record_storage = record_storage

        metadata = DatasetMetadata() if metadata is None else metadata
        version = DatasetVersion() if version is None else version

        self._record_data = DatasetRecordData() if record_data is None else record_data

        super().__init__(
            name=name,
            metadata=metadata,
            version=version
        )

    @property
    def record_data(self):
        return self._record_data

    def init(self,
             index_dimension_name: str = "id",
             uri_dimension_name: str = "uri"
             ) -> None:
        self.metadata.private_metadata.index_dimension_name = index_dimension_name
        self.metadata.private_metadata.uri_dimension_name = uri_dimension_name

        self._version_storage.init(dataset_name=self.name)
        self._object_storage.init(dataset_name=self.name)
        self._record_storage.init(dataset_name=self.name)

    def add(self,
            record_data: DatasetRecordData = None) -> None:
        self._record_data = record_data

    def commit(self, version: DatasetVersion = None, amend: bool = False) -> None:
        if version is None:
            seed = str(uuid.uuid4())
            hash_object = hashlib.sha512(bytes(seed, 'utf-8'))
            version_name = hash_object.hexdigest()[0:10]
            version = DatasetVersion(name=version_name)

        committed_version = self._version_storage.commit(dataset_name=self.name,
                                                         dataset_version=version,
                                                         dataset_metadata=self.metadata,
                                                         amend=amend)
        self._record_storage.commit(dataset_name=self.name,
                                    dataset_record_data=self.record_data,
                                    dataset_version=committed_version,
                                    index_dimension_name=self.metadata.private_metadata.index_dimension_name)

        # TODO
        #self._object_storage.commit

    def pull(self, version: DatasetVersion = None) -> None:
        pass

    def drop(self) -> None:
        self._version_storage.drop(dataset_name=self.name)
        self._object_storage.drop(dataset_name=self.name)
        self._record_storage.drop(dataset_name=self.name)

# class Dataset2(Dataset):
#     def __init__(self,
#                  name: str,
#                  storage_context: DatasetStorageContext):
#         self._data_frame = None
#         self._reference_data_frame = None
#
#
#     def init(self,
#              index_dimension_name: str = "id",
#              uri_dimension_name: str = "uri") -> None:
#         """
#
#         :param index_dimension_name:
#         :param uri_dimension_name:
#         :return:
#         :raises DatasetExistsException:
#         """
#
#         self._metadata.set_index_dimension_name(index_dimension_name=index_dimension_name)
#         self._metadata.set_uri_dimension_name(uri_dimension_name=uri_dimension_name)
#
#         self._storage_context \
#             .get_tabular_storage() \
#             .dataset_init(dataset_name=self._name)
#
#
#
#     def add(self,
#             data_frame: pandas.DataFrame) -> None:
#         self._data_frame = data_frame
#
#     # throws dataset version exists exception
#     def commit(self,
#                version_name: str = "v1",
#                overwrite_last_commit: bool = False) -> None:
#         """
#
#         :param version_name:
#         :param overwrite_last_commit:
#         :return:
#         :raises:
#             DatasetVersionExistsException
#         """
#
#         if self._data_frame is None:
#             raise NothingToCommitException()
#
#         if self._reference_data_frame is None:
#             # Either first commit (OK) or no pull executed (Not OK --> conflict in later steps)
#
#             self._storage_context.get_tabular_storage().dataset_commit(
#                 dataset_name=self._name,
#                 data_frame=self._data_frame,
#                 index_dimension_name=self._metadata.get_index_dimension_name(),
#                 uri_dimension_name=self._metadata.get_uri_dimension_name(),
#                 public_metadata=self._metadata.public_metadata,
#                 private_metadata=self._metadata.private_metadata,
#                 version_name=version_name,
#                 overwrite_last_commit=overwrite_last_commit
#             )
#
#         # if old_index_dimension == new_index_dimension:
#         #     ## index dimension not changed
#         #
#         #     old_data_dimensions = list(old_data.columns)
#         #     new_data_dimensions = list(new_data.columns)
#         #
#         #     remaining_original_dimensions = list(set(old_data_dimensions).
#         #                                          intersection(new_data_dimensions))
#         #
#         #     sorted_old_data = old_data[remaining_original_dimensions] \
#         #         .sort_values(by=remaining_original_dimensions)
#         #     sorted_new_data = new_data[remaining_original_dimensions] \
#         #         .sort_values(by=remaining_original_dimensions)
#         #
#         #     data_changed = not sorted_old_data.equals(sorted_new_data)
#         #
#         #     # TODO debug
#         #     create_new_row_based_version(dataset=dataset)
#         #
#         #     if data_changed:
#         #         ## data in remaining original dimensions changed
#         #         # --> create new row-based version
#         #
#         #         pass
#         #         # create_new_row_based_version()
#         #     else:
#         #         ## data in remaining original dimensions not changed
#         #         # --> create new column-based version
#         #
#         #         pass
#         #         # create_new_column_based_version(
#         #         #     dataset=dataset
#         #         # )
#         #
#         # else:
#         #     ## index dimension changed
#         #     # --> create new row-based version
#         #
#         #     pass
#         #     # create_new_row_based_version()
#
#     def pull(self,
#              version_name: str = "v1",
#              deep: bool = True) -> None:
#         pass
#
#     def drop(self):
#         self._storage_context\
#             .get_tabular_storage()\
#             .dataset_drop(dataset_name=self._name)
