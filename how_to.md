## Créer un venv 

```
cd jaffle_shop
python3.12 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## wsl Launch database with the docker-compose.yaml
  docker compose build mssql.configurator
  docker compose up -d mssql mssql.configurator --force-recreate


## MAC Launch database with the docker-compose.yaml
  docker-compose build mssql.configurator
  docker-compose up -d mssql mssql.configurator --force-recreate

# Load data mac et wsl

```
source .venv/bin/activate
dbt deps
dbt seed
```


It will load 6 tables of data

### Using the starter project

Try running the following commands:
- dbt run
- dbt test