# mini-holvi

## Compatibility
- Python 3.6 only. Type-hints and F-strings
- Integration tests require docker

Tested on Ubuntu 16.04 LTS.

## ETL Architecture outline

1. Changes (Insert/Update) on the source DB (PostgreSQL) are monitored by triggers
2. Changes (Insert/Update) result in Events that send the table's PK as Event ID, 
tables without a PK will sent their FK instead
3. The ETL service uses `Event ID` for synchronization between source and target
4. Target fact tables ignore deletes, are append only, 
de-normalized and partitioned by event timestamps `inserted_at` for easier querying

## How to setup

### 0. Clone the repo

### 1. Install requirements (create a venv with your favourite tool of choice)
```pip install -r requirements.txt```

### 2. Run the tests
Note: the tests download the official postgres 10 container image
```
cd mini_holvi_etl
coverage run --source=. -m unittest discover -s tests
coverage report
```

## Alternative setup (recommended)
In case docker is installed on your machine, the package/build can be simplified to the following:
```
cd deployment/service_etl
docker build -build-arg CACHEBUST=$(date +%s) -t mini_holvi_etl .
```
Now commands are available as following:  
```
docker run --net=host mini_holvi_etl manage.py [ create_database | migrate | repopulate_test_data ]
```

The demo can be run without parameters:  
```
docker run --net=host --name=testing-etl  mini_holvi_etl
```


## How to run (manually)

### 1. Start the database for tests
```docker run --net=host --name some-postgres -e POSTGRES_PASSWORD=secret -d postgres:10```

### 2. Create the initial schema and populate the db with sample data
```
python3.6 manage.py create_database
python3.6 manage.py migrate
python3.6 manage.py repopulate_test_data
```

### 3. Run the etl demo
```python3.6 mini_holvi_etl/demo.py```

From another console: ```python3.6 manage.py repopulate_test_data``` 

### (Optional) check the contents with your own eyes using pgadmin4 from this trusted source:
```
docker run --net=host --rm -p 5050:5050 thajeztah/pgadmin4
```
open the [pgadmin4 cp](localhost:5050)

![Connection](misc/pic2.png)

![Tables](misc/pic1.png)
