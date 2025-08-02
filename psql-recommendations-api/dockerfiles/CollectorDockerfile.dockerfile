FROM vladmsnk/psql-collector:latest

ENV PG_HOST=postgresdb \
    PG_PORT=5432 \
    PG_USER=user \
    PG_PASSWORD=password \
    PG_DATABASE=postgres \
    PG_SSLMODE=disable

EXPOSE 7002

CMD ["/app"]