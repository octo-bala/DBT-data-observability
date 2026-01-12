## Launch database with the docker-compose.yaml
  docker compose build mssql.configurator
  docker compose up -d mssql mssql.configurator --force-recreate
  docker compose build pg.configurator
  docker compose up -d bd_test_dbt_pg pg.configurator  --force-recreate