"""
This module...
"""
import abc

import pandas


class DatasetRecordData(abc.ABC):
    """
    This class
    """

    @property
    @abc.abstractmethod
    def record_data(self):
        """

        :return:
        """
        pass


class PandasDatasetRecordData(DatasetRecordData):
    """
    This class
    """

    @property
    @abc.abstractmethod
    def record_data(self) -> pandas.DataFrame:
        """

        :return:
        """
        pass
