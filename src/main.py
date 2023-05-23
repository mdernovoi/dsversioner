import os
from pathlib import Path

import pandas

import dsversioner as dv


index_dim_name = "myid"


def create_example_dataframe_1(observation_count=5) -> pandas.DataFrame:
    # max observation count
    assert observation_count <= 5

    data = {
        index_dim_name: list(range(100, 100 + observation_count)),
        'uri': ['f1.jpeg', 'f2.jpeg', 'f3.jpeg', 'f4.jpeg', 'f5.jpeg'][0:observation_count],
        'blaid': list(range(observation_count)),
        'somevalue': [float(item) for item in list(range(observation_count))],
    }

    df = pandas.DataFrame(data=data)
    # IMPORTANT!!!
    df = df.set_index(index_dim_name)
    df = df.sort_values(by=['blaid'], ascending=False)

    return df


def do_smth():
    # NOTE: empty record_data for type inference
    dataset_1 = dv.ObjectDataset(
        name="ds1",
        version_storage=dv.FileSystemObjectDatasetVersionStorage(
            root_path=Path(os.pardir, 'testdata')
        ),
        metadata_storage=dv.FileSystemObjectDatasetMetadataStorage(
            root_path=Path(os.pardir, 'testdata')
        ),
        object_storage=dv.FileSystemObjectDatasetObjectStorage(
            root_path=Path(os.pardir, 'testdata')
        ),
        record_storage=dv.FileSystemObjectDatasetRecordStorage(
            root_path=Path(os.pardir, 'testdata'),
            storage_format=dv.RecordStorageFormats.CSV
        ),
        record_data=dv.PandasObjectDatasetRecordData(),
        working_directory=Path(os.pardir, 'testworkdir')
    )

    # clean
    dataset_1.drop()

    # init
    dataset_1.init(index_dimension_name=index_dim_name)

    # set metadata
    dataset_1.metadata.public_metadata.set_value_by_key("bla", 15)

    # add
    dataset_1.add(record_data=dv.PandasObjectDatasetRecordData(
        record_data=create_example_dataframe_1(5)
    ))

    # commit
    committed_version = dataset_1.commit()

    # add another dataset
    dataset_1.add(record_data=dv.PandasObjectDatasetRecordData(
        record_data=create_example_dataframe_1(3)
    ))

    # commit
    dataset_1.commit(version=dv.ObjectDatasetVersion(name="1.1"))

    # redo last commit
    # NOTE: version name must be explicitly set!!!
    dataset_1.add(record_data=dv.PandasObjectDatasetRecordData(
        record_data=create_example_dataframe_1(1)
    ))
    dataset_1.commit(version=dv.ObjectDatasetVersion(name="new-1.1"), amend=True)

    # print dataset
    print(dataset_1)

    # USER: clear working dir

    # pull
    dataset_1.pull(version=dv.ObjectDatasetVersion.from_id(id=committed_version.id))

    # --------------------------------------------------------------------------------------------------------
    # new dataset with data of ds1
    dataset_2 = dv.ObjectDataset(
        name="ds1",
        version_storage=dv.FileSystemObjectDatasetVersionStorage(
            root_path=Path(os.pardir, 'testdata')
        ),
        metadata_storage=dv.FileSystemObjectDatasetMetadataStorage(
            root_path=Path(os.pardir, 'testdata')
        ),
        object_storage=dv.FileSystemObjectDatasetObjectStorage(
            root_path=Path(os.pardir, 'testdata')
        ),
        record_storage=dv.FileSystemObjectDatasetRecordStorage(
            root_path=Path(os.pardir, 'testdata'),
            storage_format=dv.RecordStorageFormats.CSV
        ),
        record_data=dv.PandasObjectDatasetRecordData(),
        working_directory=Path(os.pardir, 'testworkdir2')
    )

    # pull latest version
    dataset_2.pull()

    # add
    dataset_1.add(record_data=dv.PandasObjectDatasetRecordData(
        record_data=create_example_dataframe_1(1)
    ))

    # commit
    dataset_1.commit(version=dv.ObjectDatasetVersion(name="2.0"))


if __name__ == '__main__':
    do_smth()
