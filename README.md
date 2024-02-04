# Readme

This repository has the code of my solution to the Data Engineering Assignment

A code was built to run the ET(L) process of the given dataset. It can be found on pipeline.py.

To run the pipeline, run the following command:

`python pipeline.py --filename [filename] --write(optional)`

Where `[filename]` is be the filename (path) of the parquet file and `--write` is an optional flag. If this flag is given, the Load phase will be executed to load the resulting tables into the default database used for the exercise. If this flag is not written, the tables won’t be loaded into the default database, but will be instead saved locally as csv files.

Note: The default database used to exemplify the Load phase is a local Postgres database. It must be first initialized with Docker by typing:
`docker-compose up -d`
If the previous command is not run before setting the `--write` flag in the pipeline file, the load phase will fail.

To close the local Postgres database, then type `docker-compose down`
