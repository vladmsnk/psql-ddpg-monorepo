#!/usr/bin/env bash

MIGRATION_DIR=./migrations

DB_NAME="postgres"
DB_HOST="localhost"
DB_PORT=5432
DB_USER="user"
DB_PASS="password"

goose -dir ${MIGRATION_DIR} postgres "user=${DB_USER} dbname=${DB_NAME} password=${DB_PASS} host=${DB_HOST} port=${DB_PORT} sslmode=disable" up