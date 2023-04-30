from .Storage import TabularStorage, BlobStorage


class DatasetStorageContext:
    def __init__(self,
                 tabular_storage: TabularStorage,
                 blob_storage: BlobStorage = None):
        self._tabular_storage = tabular_storage
        self._blob_storage = blob_storage

    def get_tabular_storage(self) -> TabularStorage:
        return self._tabular_storage

    def get_blob_storage(self) -> BlobStorage:
        return self._blob_storage
