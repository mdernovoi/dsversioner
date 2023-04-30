from abc import ABC, abstractmethod

import pandas


class TabularStorage(ABC):

    @abstractmethod
    def dataset_init(self,
                     dataset_name: str) -> None:
        pass

    @abstractmethod
    def dataset_commit(self,
                       dataset_name: str,
                       data_frame: pandas.DataFrame,
                       index_dimension_name: str,
                       uri_dimension_name: str,
                       public_metadata: dict,
                       private_metadata: dict,
                       version_name: str,
                       overwrite_last_commit: bool
                       ):
        pass

    @abstractmethod
    def dataset_drop(self,
                     dataset_name: str) -> None:
        pass


class BlobStorage(ABC):
    pass


# class PostgresTabularStorage(TabularStorage):
#
#     # reference:
#     # postgres types: https://www.postgresql.org/docs/current/datatype.html
#     # pandas types:   https://pandas.pydata.org/docs/user_guide/basics.html#dtypes
#     _pandas_postgres_data_types = {
#         'object': 'text',
#         'int64': 'bigint',
#         'float64': 'double precision',
#         'bool': 'boolean',
#         'datetime64': 'timestamp',
#         'timedelta[ns]': 'text',
#         'category': 'text'
#     }
#
#
#     def __init__(self,
#                  database_connection_string: str,
#                  debug_mode: bool = False):
#         self._database_connection_string = database_connection_string
#         self._debug_mode = debug_mode
#
#     def dataset_init(self,
#                      name: str) -> None:
#
#         """
#
#         :param name:
#         :raises:
#             DatasetExistsException:
#             WrongConnectionStringFormatException:
#         :return: None
#         """
#
#         db_engine = self._create_sqlalchemy_engine()
#
#         query = f"CREATE SCHEMA {name};"
#
#         try:
#             with db_engine.connect() as connection:
#                 connection.execute(statement=text(query))
#                 connection.commit()
#         except sqlalchemy.exc.ProgrammingError as ex:
#             # The PyCharm UI does not correctly display psycopg2 errors, hence the "Cannot find reference..." warning
#             # ref: https://postgrespro.com/list/thread-id/2518318
#             #
#             # Get the original exception of sqlalchemy.exc.ProgrammingError
#             if isinstance(ex.orig, psycopg2.errors.DuplicateSchema):
#                 raise DatasetExistsException()
#
#         query = postgres_tabular_storage_dataset_init
#
#         extract_user_name_regex = r"user=([^&]+)"
#         try:
#             result = re.search(extract_user_name_regex, self._database_connection_string)
#             user_name = result.group(1)
#         except Exception as ex:
#             raise WrongConnectionStringFormatException("The connection string for postgres does not contain" +
#                                                        "a dedicated user name argument.")
#
#         prepared_query = query.replace("schema_name_placeholder", name) \
#             .replace("user_placeholder", user_name)
#
#         with db_engine.connect() as connection:
#             statement = text(prepared_query)
#             connection.execute(statement=statement)
#             connection.commit()
#
#     def dataset_commit(self,
#                        name: str,
#                        data_frame: pandas.DataFrame,
#                        public_metadata: dict,
#                        private_metadata: dict,
#                        version_name: str,
#                        overwrite_last_commit: bool
#                        ):
#
#         """
#
#         :param name:
#         :param data_frame:
#         :param public_metadata:
#         :param private_metadata:
#         :param version_name:
#         :param overwrite_last_commit:
#         :return:
#         :raises:
#             ColumnTypeMappingUndefinedException
#         """
#
#         # data_frame.to_sql() is not an option since it does not create a schema in the "append" mode
#         # and does other funny things
#
#         with self._create_sqlalchemy_engine().connect() as connection:
#
#             # create new columns
#             query = self.add_columns_query_generator(data_frame=data_frame,
#                                                      dataset_name=name)
#             statement = text(query)
#             connection.execute(statement)
#
#             # upload data
#             data_frame_rows = data_frame.to_dict('records')
#
#             # ref: https://stackoverflow.com/a/10147451
#             query = f"INSERT INTO {name}.data({', '.join(list(data_frame.columns))}) VALUES "
#             for row in data_frame_rows:
#                 values = list(row.values())
#                 # values = ['null' if value is None else f"'{str(value)}'::{type}" for value, type in zip(values, value_types)]
#                 values = ['null' if value is None else f"'{str(value)}'" for value in values]
#                 query = query + '(' + ', '.join(values) + '), '
#             # remove last comma
#             query = query[:-2]
#
#             # rows_to_insert = new_data.to_dict('records')
#             #
#             # # ref: https://stackoverflow.com/a/10147451
#             # query = f"INSERT INTO {dataset._name}.data({', '.join(list(new_data.columns))}) VALUES "
#             # for row in rows_to_insert:
#             #     values = list(row.values())
#             #     # values = ['null' if value is None else f"'{str(value)}'::{type}" for value, type in zip(values, value_types)]
#             #     values = ['null' if value is None else f"'{str(value)}'" for value in values]
#             #     query = query + '(' + ', '.join(values) + '), '
#             # # remove last comma
#             # query = query[:-2]
#             # query = query + " RETURNING observation_id;"
#             #
#             # statement = text(query)
#             # result = connection.execute(statement)
#             # inserted_observation_ids = [str(row[0]) for row in result.all()]
#             #
#
#
#
#
#
#             # # create version
#             # query_parameters = {
#             #     "version_name": new_version_name,
#             #     "version_git_commit": new_version_git_commit,
#             #     "version_message": new_version_message
#             # }
#             # query = f"""INSERT INTO {dataset._name}.versions (id, name, git_commit, message)
#             #             VALUES (DEFAULT, :version_name, :version_git_commit, :version_message)
#             #             RETURNING id;"""
#             #
#             # statement = text(query)
#             #
#             # result = connection.execute(statement, parameters=query_parameters)
#             # version_id = int(result.first()[0])
#             #
#             # # version dimensions
#             # dimension_names = list(new_data.columns)
#             # for dimension in dimension_names:
#             #     query_parameters = {
#             #         "version_id": version_id,
#             #         "dimension_name": dimension
#             #     }
#             #     query = f"""INSERT INTO {dataset._name}.version_dimensions (version_id, dimension_name)
#             #                        VALUES (:version_id, :dimension_name);"""
#             #
#             #     statement = text(query)
#             #     connection.execute(statement, parameters=query_parameters)
#             #
#             # # version observations
#             # for observation_id in inserted_observation_ids:
#             #     query_parameters = {
#             #         "version_id": version_id,
#             #         "observation_id": observation_id
#             #     }
#             #     query = f"""INSERT INTO {dataset._name}.version_observations (version_id, observation_id)
#             #                                VALUES (:version_id, :observation_id);"""
#             #
#             #     statement = text(query)
#             #     connection.execute(statement, parameters=query_parameters)
#             #
#             # # settings
#             # query_parameters = {
#             #     "index_dimension": dataset.index_dimension
#             # }
#             # query = f"""INSERT INTO {dataset._name}.settings (id, name, value, value_type)
#             #                 VALUES (DEFAULT, 'index_dimension', :index_dimension, NULL)
#             #                 RETURNING id;"""
#             #
#             # statement = text(query)
#             # result = connection.execute(statement, parameters=query_parameters)
#             # setting_ids = [str(row[0]) for row in result.all()]
#             #
#             # # version settings
#             # for setting_id in setting_ids:
#             #     query_parameters = {
#             #         "version_id": version_id,
#             #         "setting_id": setting_id
#             #     }
#             #     query = f"""INSERT INTO {dataset._name}.version_settings (version_id, setting_id)
#             #                                        VALUES (:version_id, :setting_id);"""
#             #
#             #     statement = text(query)
#             #     connection.execute(statement, parameters=query_parameters)
#
#             connection.commit()
#
#
#     def dataset_drop(self,
#                      name: str) -> None:
#         db_engine = self._create_sqlalchemy_engine()
#
#         query = f"DROP SCHEMA IF EXISTS {name} CASCADE;"
#
#         with db_engine.connect() as connection:
#             statement = text(query)
#             connection.execute(statement=statement)
#             connection.commit()
#
#     def _create_sqlalchemy_engine(self):
#         return create_engine(url=self._database_connection_string,
#                              echo=True if self._debug_mode else False,
#                              echo_pool=True if self._debug_mode else False)
#
#     def add_columns_query_generator(self,
#                                     data_frame: pandas.DataFrame,
#                                     dataset_name: str) -> str:
#         """
#
#         :param data_frame:
#         :param dataset_name:
#         :return:
#         :raises:
#             ColumnTypeMappingUndefinedException
#         """
#
#         query = str()
#         for column_name, pandas_column_type in data_frame.dtypes.items():
#
#             if str(pandas_column_type) in PostgresTabularStorage._pandas_postgres_data_types:
#                 postgres_column_type = PostgresTabularStorage._pandas_postgres_data_types[str(pandas_column_type)]
#             else:
#                 raise ColumnTypeMappingUndefinedException(column_name=column_name)
#
#             query += f"ALTER TABLE {dataset_name}.data ADD COLUMN IF NOT EXISTS {column_name} {postgres_column_type}; "
#         return query




class MinioBlobStorage(BlobStorage):
    pass



