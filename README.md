# Data Warehouse Project

This project migrates data for the fictional Sparkify app, from log files
stored in _Amazon S3_ to the _Redshift_ database.   

## Data Warehouse Goal

Sparkify decided to move their data from S3 to Redshift with the following 
goals:   

- Simplify the access to the data.
- Allow business users to access the data.
- Improve analytical query performance.
- Have the capacity to perform analytical operations over the data.

## Contents of the repo

- `sql_queries.py`: contains all the SQL statements for the project.
- `create_tables.py`:  python script that creates all the tables of the project.
- `etl.py`: python script that executes the SQL script process.
- `dwh.cfg`: example file of how the configuration should look like.

## Running the project

1. Execute the `create_tables.py` script
2. Execute the `etl.py` script 


```python
python create_tables.py
python etl.py
```
