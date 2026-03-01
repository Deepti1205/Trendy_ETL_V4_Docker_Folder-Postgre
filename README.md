# ETL : Files In Folder To Postgre Using Docker

1. Unzip the file contents
2. Run either docker build or docker compose up --build command
3. Give the command , 
docker exec -it postgres-etl psql -U etl_user -d etl_db


\dt   : shows tables
