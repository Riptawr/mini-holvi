# mini-holvi

## Compatibility
Python 3.6 only. Type-hints and F-strings

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

### 2. Start the database for tests
```docker run --net=host --name some-postgres -e POSTGRES_PASSWORD=secret -d postgres:10```

### 3. Run the tests
```
cd mini_holvi_etl
coverage run --source=. -m unittest discover -s tests
coverage report
```

### 4. Create the initial schema and populate the db with sample data
```
python manage.py create_database
python manage.py migrate
python manage.py repopulate_test_data
```

### 5. Run the etl demo
```python mini_holvi_etl/demo.py```

From another console: ```python manage.py repopulate_test_data``` 

### 6. (Optional) check the contents with your own eyes using pgadmin4 from this trusted source:
```
docker run --net=host --rm -p 5050:5050 thajeztah/pgadmin4
```
open the [pgadmin4 cp](localhost:5050)

![Connection](misc/pic2.png)

![Tables](misc/pic1.png)
