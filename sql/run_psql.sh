#!/bin/bash
#
#
HOST=fellows-dev-dw-pgsql.c8nmhr4y8dps.us-east-1.rds.amazonaws.com
USER=dwaddle
DB=dw_pgsql

psql --host=$HOST --username=$USER $DB
