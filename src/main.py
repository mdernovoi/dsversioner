import os
from pathlib import Path

import pandas

import dsversioner as dv

#tabular-storage = PostgresTabularStorage(connection string, secrets, user, ...)
#blob-storage = MinioBlobStorage(url, ...)

#storage_context = Storage(TabularStorage, BlobStorage: opt)

# dataset = Dataset("name", storage_context)
# dataset.init(index-col)
# dataset.add(mydataframe)
# dataset.commit()
#
# dataset.add(newdataframe)
# dataset.commit("v147")
#
# dataset2 = Dataset("myname", storage_context)
# dataset2.pull("v1.2.3")
#
# dataset2.add(mynewnwedatafarme)
# dataset2.commit()

index_dim_name = "ddd"
def create_example_dataframe_1() -> pandas.DataFrame:

    observation_count = 5

    data = {
        index_dim_name: list(range(100, 100+observation_count)),
        'uri': ['f1.jpeg', 'f2.jpeg', 'f3.jpeg', 'f4.jpeg', 'f5.jpeg'],
        'blaid': list(range(observation_count)),
        'somevalue': [float(item) for item in list(range(observation_count))],
    }

    df = pandas.DataFrame(data=data)
    # IMPORTANT!!!
    df = df.set_index(index_dim_name)
    df = df.sort_values(by=['blaid'], ascending=False)

    return df


def do_smth():
    #filesys_store_ctxt = dv.FileSystemTabularStorage(path=str(Path("dataset_tests")))
    #dataset_storage_context = dv.DatasetStorageContext(tabular_storage=filesys_store_ctxt)

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
        working_directory=Path(os.pardir, 'testworkdir'),
        #record_data=dv.PandasObjectDatasetRecordData()
    )

    #dataset_1 = dv.Dataset(name="ds1", storage_context=dataset_storage_context)

    # try:
    #     dataset_1.init()
    # except dv.DatasetExistsException:
    #     dataset_1.drop()
    #     dataset_1.init()


    dataset_1.add(record_data=dv.PandasObjectDatasetRecordData(
        record_data=create_example_dataframe_1()
    ))
    dataset_1.metadata.public_metadata.set_value_by_key("bla", "uz7")
    dataset_1.metadata.private_metadata.index_dimension_name = index_dim_name
    dataset_1.metadata.private_metadata.uri_dimension_name = "uri"
    dataset_1.commit(version=dv.ObjectDatasetVersion(name="ddw"), amend=False)


    #dataset_1.pull()
    i = 5
    # NOTE: only id as filter criterion is supported
    #dataset_1.pull(version=dv.DatasetVersion(id=8))
    i = 5


if __name__ == '__main__':
    do_smth()