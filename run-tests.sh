#!/bin/sh -e
# To execute first export URL pointing at a running MongoDB deployment
# export URL="mongodb+srv://myuser:mypasswd@mycluster.abc.mongodb.net/test"

DEFAULT_URL="mongodb://localhost:27017/test"
: "${URL:=${DEFAULT_URL}}"

printf "STARTING TESTS, CONNECTING TO: ${URL}\n\n"

mongosh ${URL} --eval "
  load('mongo-agg-extract-schema.js');
  runAllTests();
"

printf "TESTS ALL EXECUTED SUCCESSFULLY\n"
