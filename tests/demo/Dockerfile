FROM postgres:9.6-alpine

EXPOSE 5432

ENV POSTGRES_USER=sodasql
ENV POSTGRES_DB=sodasql
ENV POSTGRES_HOST_AUTH_METHOD=trust

COPY ./demodata.sql /docker-entrypoint-initdb.d/

CMD ["postgres"]
