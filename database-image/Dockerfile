# Use a lightweight Java image
FROM mysql:8.3.0
LABEL maintainer="qleoz12 <kintaroloop@gmail.com>"


COPY ./init.sql /docker-entrypoint-initdb.d

EXPOSE 3306