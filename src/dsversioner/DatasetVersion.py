
class DatasetVersion:
    def __init__(self,
                 version_id: int,
                 version_name: str):
        self._version_id = version_id
        self._version_name = version_name

    @property
    def version_id(self):
        return self._version_id

    @version_id.setter
    def version_id(self, version_id):
        self._version_id = version_id

    @property
    def version_name(self):
        return self._version_name

    @version_name.setter
    def version_name(self, version_name):
        self._version_name = version_name
