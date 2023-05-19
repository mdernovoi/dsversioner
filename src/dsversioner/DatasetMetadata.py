from abc import abstractmethod, ABCMeta, abstractclassmethod


class PrivateDatasetMetadata(metaclass=ABCMeta):

    @abstractmethod
    def to_json(self):
        pass

    @classmethod
    @abstractmethod
    def from_json(cls, json_dict):
        pass


class PublicDatasetMetadata:
    def __init__(self,
                 data=None):
        self._data = dict() if data is None else data

    def get_value_by_key(self,
                         key: str):
        return self._data[key]

    def set_value_by_key(self,
                         key: str,
                         value: str) -> None:
        self._data[key] = value

    def to_json(self):
        return self._data

    @classmethod
    def from_json(cls, json_dict):
        return PublicDatasetMetadata(data=json_dict)


class DatasetMetadata(metaclass=ABCMeta):

    @property
    @abstractmethod
    def private_metadata(self):
        pass

    @property
    @abstractmethod
    def public_metadata(self):
        pass

    @abstractmethod
    def to_json(self):
        pass

    @classmethod
    @abstractmethod
    def from_json(cls, json_dict):
        pass


class PrivateObjectDatasetMetadata(PrivateDatasetMetadata):
    def __init__(self,
                 index_dimension_name: str = None,
                 uri_dimension_name: str = None,
                 record_storage_data_location: str = None
                 ):
        self._index_dimension_name = index_dimension_name
        self._uri_dimension_name = uri_dimension_name
        self._record_storage_data_location = record_storage_data_location

    @property
    def index_dimension_name(self):
        return self._index_dimension_name

    @index_dimension_name.setter
    def index_dimension_name(self, index_dimension_name):
        self._index_dimension_name = index_dimension_name

    @property
    def uri_dimension_name(self):
        return self._uri_dimension_name

    @uri_dimension_name.setter
    def uri_dimension_name(self, uri_dimension_name):
        self._uri_dimension_name = uri_dimension_name

    @property
    def record_storage_data_location(self):
        return self._record_storage_data_location

    @record_storage_data_location.setter
    def record_storage_data_location(self, record_storage_data_location):
        self._record_storage_data_location = record_storage_data_location

    def to_json(self):
        to_return = {
            "index_dimension_name": self._index_dimension_name,
            "uri_dimension_name": self._uri_dimension_name,
            "record_storage_data_location": self._record_storage_data_location
        }
        return to_return

    @classmethod
    def from_json(cls, json_dict):
        return PrivateObjectDatasetMetadata(
            index_dimension_name=json_dict['index_dimension_name'],
            uri_dimension_name=json_dict['uri_dimension_name'],
            record_storage_data_location=json_dict['record_storage_data_location'])


class ObjectDatasetMetadata(DatasetMetadata):

    def __init__(self,
                 private_metadata: PrivateObjectDatasetMetadata = None,
                 public_metadata: PublicDatasetMetadata = None):

        self._private_metadata = private_metadata
        if self._private_metadata is None:
            self._private_metadata = PrivateObjectDatasetMetadata()

        self._public_metadata = public_metadata
        if self._public_metadata is None:
            self._public_metadata = PublicDatasetMetadata()

    @property
    def private_metadata(self):
        return self._private_metadata

    @property
    def public_metadata(self):
        return self._public_metadata

    def to_json(self):
        to_return = {
            "private_metadata": self._private_metadata,
            "public_metadata": self._public_metadata
        }
        return to_return

    @classmethod
    def from_json(cls, json_dict):
        if json_dict is not None:
            return ObjectDatasetMetadata(
                private_metadata=PrivateObjectDatasetMetadata.from_json(json_dict['private_metadata']),
                public_metadata=PublicDatasetMetadata.from_json(json_dict['public_metadata']))
        else:
            return ObjectDatasetMetadata()




