class DatasetExistsException(Exception):
    def __init__(self,
                 message="The dataset already exists."):
        self.message = message
        super().__init__(self.message)


class DatasetVersionExistsException(Exception):
    def __init__(self,
                 message="The version of the dataset already exists."):
        self.message = message
        super().__init__(self.message)

class WrongConnectionStringFormatException(Exception):
    def __init__(self,
                 message="The format of the connection string is not valid."):
        self.message = message
        super().__init__(self.message)

class ColumnTypeMappingUndefinedException(Exception):
    def __init__(self,
                 column_name="",
                 message=f"Could not convert the data type of column: ."):
        self.message = message + column_name
        super().__init__(self.message)

class NothingToCommitException(Exception):
    def __init__(self,
                 message=f"There is nothing to commit. Try add(data_frame) first."):
        self.message = message
        super().__init__(self.message)

