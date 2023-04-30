import pandas

from .DatasetStorageContext import DatasetStorageContext
from .DatasetMetadata import *
from .Errors import *



class Dataset:
    def __init__(self,
                 name: str,
                 storage_context: DatasetStorageContext):
        self._name = name
        self._storage_context = storage_context
        self._metadata = DatasetMetadata()
        self._data_frame = None
        self._reference_data_frame = None


    def init(self,
             index_dimension_name: str = "id",
             uri_dimension_name: str = "uri") -> None:
        """

        :param index_dimension_name:
        :param uri_dimension_name:
        :return:
        :raises DatasetExistsException:
        """

        self._metadata.set_index_dimension_name(index_dimension_name=index_dimension_name)
        self._metadata.set_uri_dimension_name(uri_dimension_name=uri_dimension_name)

        self._storage_context \
            .get_tabular_storage() \
            .dataset_init(dataset_name=self._name)



    def add(self,
            data_frame: pandas.DataFrame) -> None:
        self._data_frame = data_frame

    # throws dataset version exists exception
    def commit(self,
               version_name: str = "v1",
               overwrite_last_commit: bool = False) -> None:
        """

        :param version_name:
        :param overwrite_last_commit:
        :return:
        :raises:
            DatasetVersionExistsException
        """

        if self._data_frame is None:
            raise NothingToCommitException()

        if self._reference_data_frame is None:
            # Either first commit (OK) or no pull executed (Not OK --> conflict in later steps)

            self._storage_context.get_tabular_storage().dataset_commit(
                dataset_name=self._name,
                data_frame=self._data_frame,
                index_dimension_name=self._metadata.get_index_dimension_name(),
                uri_dimension_name=self._metadata.get_uri_dimension_name(),
                public_metadata=self._metadata.public_metadata,
                private_metadata=self._metadata.private_metadata,
                version_name=version_name,
                overwrite_last_commit=overwrite_last_commit
            )

        # if old_index_dimension == new_index_dimension:
        #     ## index dimension not changed
        #
        #     old_data_dimensions = list(old_data.columns)
        #     new_data_dimensions = list(new_data.columns)
        #
        #     remaining_original_dimensions = list(set(old_data_dimensions).
        #                                          intersection(new_data_dimensions))
        #
        #     sorted_old_data = old_data[remaining_original_dimensions] \
        #         .sort_values(by=remaining_original_dimensions)
        #     sorted_new_data = new_data[remaining_original_dimensions] \
        #         .sort_values(by=remaining_original_dimensions)
        #
        #     data_changed = not sorted_old_data.equals(sorted_new_data)
        #
        #     # TODO debug
        #     create_new_row_based_version(dataset=dataset)
        #
        #     if data_changed:
        #         ## data in remaining original dimensions changed
        #         # --> create new row-based version
        #
        #         pass
        #         # create_new_row_based_version()
        #     else:
        #         ## data in remaining original dimensions not changed
        #         # --> create new column-based version
        #
        #         pass
        #         # create_new_column_based_version(
        #         #     dataset=dataset
        #         # )
        #
        # else:
        #     ## index dimension changed
        #     # --> create new row-based version
        #
        #     pass
        #     # create_new_row_based_version()

    def pull(self,
             version_name: str = "v1",
             deep: bool = True) -> None:
        pass

    def drop(self):
        self._storage_context\
            .get_tabular_storage()\
            .dataset_drop(dataset_name=self._name)
