# dataset-versioner

## todo

- use bigint for every index column (not int)
- replace psycopg-binary with psycopg compiled from source
	- in dev-python-r environment
	- in api project requirements.txt
- use sqlalchemy as a proper orm and not only to execute raw sql
- add proper settings section to DataSet class from the api and synchronize it with the settings table
- rename settings into metadata in db schema and api DataSet class
- rewrite get_dataset() to use less single queries
- replace all "id" columns with "somethingsomething_id" for better readability in db schema

