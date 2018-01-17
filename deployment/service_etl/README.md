## How to build

#### 1. Building the container (from this directory)
```docker build -build-arg CACHEBUST=$(date +%s) -t mini_holvi_etl .```

#### 2. Running locally
To start demo.py:
```
docker run --net=host --name=mini-holvi-etl -d mini_holvi_etl:latest
```

For management commands, just pass the command:

```
docker run --net=host mini_holvi_etl manage.py [create_database | migrate | repopulate_test_data]
```

