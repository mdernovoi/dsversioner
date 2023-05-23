"""
This module
"""

from .DatasetMetadata import DatasetMetadata, PrivateDatasetMetadata, PublicKeyValueDatasetMetadata
from .Serializable import JsonSerializable


class PrivateObjectDatasetMetadata(PrivateDatasetMetadata, JsonSerializable):
    """
    This class
    """

    def __init__(self,
                 index_dimension_name: str = None,
                 uri_dimension_name: str = None,
                 record_storage_data_location: str = None,
                 object_storage_data_location: str = None
                 ):
        self._index_dimension_name = index_dimension_name
        self._uri_dimension_name = uri_dimension_name
        self._record_storage_data_location = record_storage_data_location
        self._object_storage_data_location = object_storage_data_location

    @property
    def index_dimension_name(self) -> str:
        """

        :return:
        """
        return self._index_dimension_name

    @index_dimension_name.setter
    def index_dimension_name(self,
                             index_dimension_name: str) -> None:
        """

        :param index_dimension_name:
        :return:
        """
        self._index_dimension_name = index_dimension_name

    @property
    def uri_dimension_name(self) -> str:
        """

        :return:
        """
        return self._uri_dimension_name

    @uri_dimension_name.setter
    def uri_dimension_name(self,
                           uri_dimension_name: str) -> None:
        """

        :param uri_dimension_name:
        :return:
        """
        self._uri_dimension_name = uri_dimension_name

    @property
    def record_storage_data_location(self) -> str:
        """

        :return:
        """
        return self._record_storage_data_location

    @record_storage_data_location.setter
    def record_storage_data_location(self,
                                     record_storage_data_location: str) -> None:
        """

        :param record_storage_data_location:
        :return:
        """
        self._record_storage_data_location = record_storage_data_location

    @property
    def object_storage_data_location(self) -> str:
        """

        :return:
        """
        return self._object_storage_data_location

    @object_storage_data_location.setter
    def object_storage_data_location(self,
                                     object_storage_data_location: str) -> None:
        """

        :param object_storage_data_location:
        :return:
        """
        self._object_storage_data_location = object_storage_data_location

    def to_json(self) -> dict:
        to_return = {
            "index_dimension_name": self._index_dimension_name,
            "uri_dimension_name": self._uri_dimension_name,
            "record_storage_data_location": self._record_storage_data_location,
            "object_storage_data_location": self._object_storage_data_location
        }
        return to_return

    @classmethod
    def from_json(cls,
                  json_dict: dict):  # TODO: uncomment after migration to python 3.11 -> Self:

        if json_dict is None:
            return PrivateObjectDatasetMetadata()

        return PrivateObjectDatasetMetadata(
            index_dimension_name=json_dict['index_dimension_name'],
            uri_dimension_name=json_dict['uri_dimension_name'],
            record_storage_data_location=json_dict['record_storage_data_location'],
            object_storage_data_location=json_dict['object_storage_data_location'])


class PublicKeyValueObjectDatasetMetadata(PublicKeyValueDatasetMetadata, JsonSerializable):

    def to_json(self) -> dict:
        """

        :return:
        """
        return self.data

    @classmethod
    def from_json(cls,
                  json_dict: dict):  # TODO: uncomment after migration to python 3.11 -> Self:
        """

        :param json_dict:
        :return:
        """
        return PublicKeyValueObjectDatasetMetadata(data=json_dict)


class ObjectDatasetMetadata(DatasetMetadata, JsonSerializable):

    def __init__(self,
                 private_metadata: PrivateObjectDatasetMetadata = None,
                 public_metadata: PublicKeyValueObjectDatasetMetadata = None):

        self._private_metadata = private_metadata
        if self._private_metadata is None:
            self._private_metadata = PrivateObjectDatasetMetadata()

        self._public_metadata = public_metadata
        if self._public_metadata is None:
            self._public_metadata = PublicKeyValueObjectDatasetMetadata()

    @property
    def private_metadata(self) -> PrivateObjectDatasetMetadata:
        """

        :return:
        """
        return self._private_metadata

    @property
    def public_metadata(self) -> PublicKeyValueObjectDatasetMetadata:
        """

        :return:
        """
        return self._public_metadata

    def to_json(self) -> dict:
        to_return = {
            "private_metadata": self._private_metadata,
            "public_metadata": self._public_metadata
        }
        return to_return

    @classmethod
    def from_json(cls,
                  json_dict: dict):  # TODO: uncomment after migration to python 3.11 -> Self:

        if json_dict is None:
            return ObjectDatasetMetadata()

        return ObjectDatasetMetadata(
            private_metadata=PrivateObjectDatasetMetadata.from_json(json_dict['private_metadata']),
            public_metadata=PublicKeyValueObjectDatasetMetadata.from_json(json_dict['public_metadata']))
