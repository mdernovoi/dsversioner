import copy

from pandas import DataFrame


class Dataset:
  def __init__(self,
               name: str,
               data: DataFrame,
               index_dimension: str,
               version: int):
    self.name = name
    self.data = data
    self.__old_data = copy.deepcopy(data)
    self.index_dimension = index_dimension
    self.__old_index_dimension = index_dimension
    self.version = version

  def get_old_data(self) -> DataFrame:
    return self.__old_data

  def get_old_index_dimenstion(self) -> str:
    return self.__old_index_dimension