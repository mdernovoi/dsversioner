class PrivateDatasetMetadata:
    def __init__(self,
                 index_dimension_name: str = None,
                 uri_dimension_name: str = None
                 ):
        self._index_dimension_name = index_dimension_name
        self._uri_dimension_name = uri_dimension_name

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

    def to_json(self):
        to_return = {
            "index_dimension_name": self._index_dimension_name,
            "uri_dimension_name": self._uri_dimension_name
        }
        return to_return

    @classmethod
    def from_json(cls, json_dict):
        return PrivateDatasetMetadata(
            index_dimension_name=json_dict['index_dimension_name'],
            uri_dimension_name=json_dict['uri_dimension_name'])


class PublicDatasetMetadata:
    def __init__(self,
                 data = None):
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


class DatasetMetadata:
    def __init__(self,
                 private_metadata: PrivateDatasetMetadata = None,
                 public_metadata: PublicDatasetMetadata = None):
        self._private_metadata = PrivateDatasetMetadata() if private_metadata is None else private_metadata
        self._public_metadata = PublicDatasetMetadata() if public_metadata is None else public_metadata

    @property
    def private_metadata(self):
        return self._private_metadata

    @private_metadata.setter
    def private_metadata(self, private_metadata):
        self._private_metadata = private_metadata

    @property
    def public_metadata(self):
        return self._public_metadata

    @public_metadata.setter
    def public_metadata(self, public_metadata):
        self._public_metadata = public_metadata

    def to_json(self):
        to_return = {
            "private_metadata": self._private_metadata,
            "public_metadata": self._public_metadata
        }
        return to_return

    @classmethod
    def from_json(cls, json_dict):
        return DatasetMetadata(
            private_metadata=PrivateDatasetMetadata.from_json(json_dict['private_metadata']),
            public_metadata=PublicDatasetMetadata.from_json(json_dict['public_metadata']))