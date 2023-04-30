from copy import deepcopy


class DatasetMetadata:
    def __init__(self,
                 private_metadata: dict = None,
                 public_metadata: dict = None):
        self._private_metadata = dict() if private_metadata is None else private_metadata
        self._public_metadata = dict() if public_metadata is None else public_metadata

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


    def get_index_dimension_name(self):
        return self.private_metadata["index_dimension_name"] \
            if "index_dimension_name" in self.private_metadata else None

    def set_index_dimension_name(self, index_dimension_name):
        self.private_metadata["index_dimension_name"] = index_dimension_name

    def get_uri_dimension_name(self):
        return self.private_metadata["uri_dimension_name"]\
            if "uri_dimension_name" in self.private_metadata else None

    def set_uri_dimension_name(self, uri_dimension_name):
        self.private_metadata["uri_dimension_name"] = uri_dimension_name

    def _set_private_metadata_value(self,
                                    key: str,
                                    value: str) -> None:
        self.private_metadata[key] = value

    def set_public_metadata_value(self,
                                  key: str,
                                  value: str) -> None:
        self.public_metadata[key] = value

