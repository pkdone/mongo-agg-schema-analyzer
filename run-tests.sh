#!/bin/sh -e
# To execute first export URL pointing at a running MongoDB deployment
# export URL="mongodb+srv://main_user:Password1@aggcluster.s703u.mongodb.net/test"

DEFAULT_URL="mongodb://localhost:27017/test"
: "${URL:=${DEFAULT_URL}}"

printf "Connecting mongosh to: ${URL}\n\n"

mongosh ${URL} --eval "
  load('mongo-agg-extract-schema.js');
  runAllTests();
"

printf "FINISHED SUCCESSFULLY\n"
