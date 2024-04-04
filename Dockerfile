FROM mysql:latest

ENV MYSQL_ROOT_PASSWORD=root 

COPY ./database_historical.sql /docker-entrypoint-initdb.d/