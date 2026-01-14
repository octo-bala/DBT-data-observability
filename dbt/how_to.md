## Créer un venv
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

## wsl Launch database with the docker-compose.yaml
  docker compose build mssql.configurator
  docker compose up -d mssql mssql.configurator --force-recreate


## MAC Launch database with the docker-compose.yaml
  docker-compose build mssql.configurator
  docker-compose up -d mssql mssql.configurator --force-recreate

