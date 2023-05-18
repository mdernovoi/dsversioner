class DatasetExistsException(Exception):
    def __init__(self,
                 dataset_name: str,
                 message: str = "The dataset already exists.") -> None:
        self.message = f"{message} Dataset name: {dataset_name}."
        super().__init__(self.message)


class DatasetDoesNotExistException(Exception):
    def __init__(self,
                 dataset_name: str,
                 message: str = "The dataset does not exists.") -> None:
        self.message = f"{message} Dataset name: {dataset_name}."
        super().__init__(self.message)


class DatasetVersionExistsException(Exception):
    def __init__(self,
                 dataset_name: str,
                 dataset_version_name: str,
                 message: str = "The version of the dataset already exists.") -> None:
        self.message = f"{message} Dataset name: {dataset_name}. Dataset version: {dataset_version_name}."
        super().__init__(self.message)


class DatasetVersionDoesNotExistException(Exception):
    def __init__(self,
                 dataset_name: str,
                 dataset_version_name: str,
                 message: str = "The version of the dataset does not exist.") -> None:
        self.message = f"{message} Dataset name: {dataset_name}. Dataset version: {dataset_version_name}."
        super().__init__(self.message)


class NothingToCommitException(Exception):
    def __init__(self,
                 dataset_name: str,
                 message: str = f"There is nothing to commit. Try add(data_frame) first.") -> None:
        self.message = f"{message} Dataset name: {dataset_name}."
        super().__init__(self.message)


class NothingToPullException(Exception):
    def __init__(self,
                 dataset_name: str,
                 message: str = f"There is nothing to pull. Try commit() first.") -> None:
        self.message = f"{message} Dataset name: {dataset_name}."
        super().__init__(self.message)


class InvalidStorageLocationException(Exception):
    def __init__(self,
                 location: str,
                 message: str = f"The specified storage location is invalid.") -> None:
        self.message = f"{message} Storage location: {location}"
        super().__init__(self.message)

class InvalidFileSystemStorageFormatException(Exception):
    def __init__(self,
                 dataset_name: str,
                 message: str = f"The specified storage format is invalid.") -> None:
        self.message = f"{message} Dataset: {dataset_name}"
        super().__init__(self.message)


