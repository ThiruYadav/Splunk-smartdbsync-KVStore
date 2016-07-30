#!/bin/bash
#
#
HOST=localhost
USER=dwaddle
DB=postgres

psql -f `basename $0 .sh`.sql --host=$HOST --username=$USER $DB
