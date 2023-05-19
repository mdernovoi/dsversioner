import copy
import hashlib
import os
import random
import uuid
from datetime import datetime
from pathlib import Path

import pandas
import sqlalchemy.exc
from dotenv import load_dotenv
from pandas import DataFrame
from sqlalchemy import create_engine, Engine
from sqlalchemy.sql import text

from Data import Dataset


def get_database_engine() -> Engine:

    dotenv_path = Path("secrets", "database_connection.env")
    load_dotenv(dotenv_path=dotenv_path)

    connection_string = os.getenv('DATABASE_CONNECTION_STRING')

    engine = create_engine(url=connection_string,
                           echo=True,
                           echo_pool=True)
    return engine


# throws exception if dataset already exists
def create_dataset(name: str,
                   user: str = "postgres") -> None:

    engine = get_database_engine()

    with open(Path("sql", "create_dataset.sql"), 'r') as file:
        query = file.read()


    prepared_query = query.replace("schema_name_placeholder", name)\
                          .replace("user_placeholder", user)


    with engine.connect() as connection:

        statement = text(prepared_query)
        connection.execute(statement=statement)
        connection.commit(,,,

def delete_dataset(name: str) -> None:

    engine = get_database_engine()

    query = "DROP SCHEMA IF EXISTS schema_name_placeholder CASCADE;"

    prepared_query = query.replace("schema_name_placeholder", name)

    with engine.connect() as connection:
        statement = text(prepared_query)
        connection.execute(statement=statement)
        connection.commit(,,,

# raises exception
def get_database_type_from_pandas_type(pandas_type: str) -> str:

    if pandas_type == 'object':
        return 'text'
    if pandas_type == 'int64':
        return 'bigint'
    if pandas_type == 'float64':
        return 'double precision'
    if pandas_type == 'bool':
        return 'boolean'
    if pandas_type == 'datetime64':
        return 'timestamp'
    if pandas_type == 'timedelta[ns]':
        return 'text'
    if pandas_type == 'category':
        return 'text'

    raise Exception("Can not convert pandas to database column type.")

def create_new_row_based_version(
        dataset: Dataset,
        new_version_name: str = "",
        new_version_git_commit: str = "",
        new_version_message: str = ""
) -> None:

    new_data = copy.deepcopy(dataset.data)
    new_index_dimension = dataset.index_dimension
    new_data_dimensions = list(new_data.columns)

    #seed = datetime.utcnow().isoformat() + '+' + str(random.random())
    seed = str(uuid.uuid4())
    hash_object = hashlib.sha512(bytes(seed, 'utf-8'))
    postfix = '_' + hash_object.hexdigest()[0:10]

    system_dimension_names = ['observation_type',
                              'observation_tags',
                              'observation_uri']

    old_column_names = list(new_data.columns)
    new_column_names = list()
    # names of system columns remain same across all dataset versions
    for column_name in old_column_names:
        if not column_name in system_dimension_names:
            modified_column_name = column_name + postfix
        else:
            modified_column_name = column_name
        new_column_names.append(modified_column_name)
    new_column_names = dict(zip(old_column_names, new_column_names))
    new_data = new_data.rename(columns=new_column_names)

    new_data_data_types =  new_data.dtypes.to_dict()

    engine = get_database_engine()

    with engine.connect() as connection:

        # create new columns
        for key, value in new_data_data_types.items():
            column_name = key
            column_type = str(value)

            database_column_type = get_database_type_from_pandas_type(pandas_type=column_type)

            query = f"ALTER TABLE {dataset._name}.data ADD COLUMN IF NOT EXISTS {column_name} {database_column_type};"
            statement = text(query)

            connection.execute(statement)

        # upload data

        # new_data.to_sql(
        #     name="data",
        #     schema="mydata",
        #     con=connection,
        #     if_exists='append',
        #     index=False
        # )

        rows_to_insert = new_data.to_dict('records')

        # ref: https://stackoverflow.com/a/10147451
        query = f"INSERT INTO {dataset._name}.data({', '.join(list(new_data.columns))}) VALUES "
        for row in rows_to_insert:
            values = list(row.values())
            #values = ['null' if value is None else f"'{str(value)}'::{type}" for value, type in zip(values, value_types)]
            values = ['null' if value is None else f"'{str(value)}'" for value in values]
            query = query + '(' + ', '.join(values) + '), '
        # remove last comma
        query = query[:-2]
        query = query + " RETURNING observation_id;"

        statement = text(query)
        result = connection.execute(statement)
        inserted_observation_ids = [str(row[0]) for row in result.all()]


        # create version
        query_parameters = {
            "version_name": new_version_name,
            "version_git_commit": new_version_git_commit,
            "version_message": new_version_message
        }
        query = f"""INSERT INTO {dataset._name}.versions (id, name, git_commit, message)
                    VALUES (DEFAULT, :version_name, :version_git_commit, :version_message)
                    RETURNING id;"""

        statement = text(query)

        result = connection.execute(statement, parameters=query_parameters)
        version_id = int(result.first()[0])

        # version dimensions
        dimension_names = list(new_data.columns)
        for dimension in dimension_names:
            query_parameters = {
                "version_id": version_id,
                "dimension_name": dimension
            }
            query = f"""INSERT INTO {dataset._name}.version_dimensions (version_id, dimension_name)
                               VALUES (:version_id, :dimension_name);"""

            statement = text(query)
            connection.execute(statement, parameters=query_parameters)

        # version observations
        for observation_id in inserted_observation_ids:
            query_parameters = {
                "version_id": version_id,
                "observation_id": observation_id
            }
            query = f"""INSERT INTO {dataset._name}.version_observations (version_id, observation_id)
                                       VALUES (:version_id, :observation_id);"""

            statement = text(query)
            connection.execute(statement, parameters=query_parameters)

        # settings
        query_parameters = {
            "index_dimension": dataset.index_dimension
        }
        query = f"""INSERT INTO {dataset._name}.settings (id, name, value, value_type)
                        VALUES (DEFAULT, 'index_dimension', :index_dimension, NULL)
                        RETURNING id;"""

        statement = text(query)
        result = connection.execute(statement, parameters=query_parameters)
        setting_ids = [str(row[0]) for row in result.all()]

        # version settings
        for setting_id in setting_ids:
            query_parameters = {
                "version_id": version_id,
                "setting_id": setting_id
            }
            query = f"""INSERT INTO {dataset._name}.version_settings (version_id, setting_id)
                                               VALUES (:version_id, :setting_id);"""

            statement = text(query)
            connection.execute(statement, parameters=query_parameters)

        connection.commit(,,,

    i = 5

    ## get existing columns in database

def commit_dataset(
        dataset: Dataset,
        new_version_name: str = "",
        new_version_git_commit: str = "",
        new_version_message: str = ""
) -> None:

    old_data = dataset.get_old_data()
    new_data = dataset.data

    old_index_dimension = dataset.get_old_index_dimenstion()
    new_index_dimension = dataset.index_dimension

    if old_index_dimension == new_index_dimension:
        ## index dimension not changed

        old_data_dimensions = list(old_data.columns)
        new_data_dimensions = list(new_data.columns)

        remaining_original_dimensions = list(set(old_data_dimensions).
                                             intersection(new_data_dimensions))

        sorted_old_data = old_data[remaining_original_dimensions]\
            .sort_values(by=remaining_original_dimensions)
        sorted_new_data = new_data[remaining_original_dimensions]\
            .sort_values(by=remaining_original_dimensions)

        data_changed = not sorted_old_data.equals(sorted_new_data)

        # TODO debug
        create_new_row_based_version(dataset=dataset)

        if data_changed:
            ## data in remaining original dimensions changed
            # --> create new row-based version

            pass
            # create_new_row_based_version()
        else:
            ## data in remaining original dimensions not changed
            # --> create new column-based version

            pass
            # create_new_column_based_version(
            #     dataset=dataset
            # )

    else:
        ## index dimension changed
        # --> create new row-based version

        pass
        #create_new_row_based_version()

def get_dataset(
        name: str,
        version: str = 'latest'
) -> Dataset:

    engine = get_database_engine()

    # version
    if version.strip().lower() == 'latest':

        with engine.connect() as connection:
            query = f"""SELECT max(id) 
                        FROM {name}.versions;"""
            statement = text(query)
            result = connection.execute(statement)
            version = int(result.first()[0])

    # settings
    with engine.connect() as connection:
        query = f"""SELECT s.name, s.value, s.value_type
                    FROM {name}.settings s INNER JOIN {name}.version_settings v ON s.id = v.setting_id
                    WHERE version_id = {version};"""
        statement = text(query)
        result = connection.execute(statement)
        settings = [{"name": row[0],
                     "value": row[1],
                     "value_type": row[2]} for row in result.all()]

    # dimensions
    with engine.connect() as connection:
        query = f"""SELECT dimension_name 
                    FROM {name}.version_dimensions
                    WHERE version_id = {version};"""
        statement = text(query)
        result = connection.execute(statement)
        dimensions = [str(row[0]) for row in result.all()]

    # observations
    with engine.connect() as connection:
        query = f"""SELECT {', '.join(dimensions)} 
                    FROM {name}.data d INNER JOIN {name}.version_observations v ON d.observation_id = v.observation_id
                    WHERE version_id = {version};"""
        statement = text(query)

        dataframe = pandas.read_sql_query(
            sql=statement,
            con=connection,
            index_col=None
        )

    index_dimension = None
    for setting in settings:
        if setting['name'] == 'index_dimension':
            index_dimension = setting['value']

    dataset = Dataset(
        name=name,
        version=version,
        index_dimension=index_dimension,
        data=dataframe
    )

    return dataset

def create_example_dataset_1() -> Dataset:

    observation_count = 10

    data = {
        'observation_type': [None] * observation_count,
        'observation_tags': ['{ "value": 5 }'] * observation_count,
        'observation_uri': ['somestring'] * observation_count,
        'image_id': list(range(observation_count)),
        'somevalue': [float(item) for item in list(range(observation_count))],
    }

    df = DataFrame(data=data)
    df = df.sort_values(by=['image_id'], ascending=False)

    dataset = Dataset(
        name = "mydata",
        data=df,
        index_dimension="image_id",
        version=None
    )

    return dataset

def do_stuff():


    dotenv_path = Path("secrets","database_connection.env")
    load_dotenv(dotenv_path=dotenv_path)
    connection_string = os.getenv('DATABASE_CONNECTION_STRING')
    engine = create_engine(url = connection_string,
                           echo=False,
                           echo_pool=False)

    with engine.connect() as con:
        statement = text("""SELECT * FROM schema_name_placeholder.data""")
        rs = con.execute(statement=statement)

        for row in rs:
            print(row)

    i = 5


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #delete_dataset("mydata")
    #create_dataset("mydata")
    dataset = create_example_dataset_1()
    #commit_dataset(dataset=dataset)
    get_dataset('mydata')
