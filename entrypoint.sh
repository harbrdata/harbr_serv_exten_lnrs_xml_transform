#!/bin/sh
set -ex

# Option 1: Pass the environment variables as arguments to the Python CLI
cmd="poetry run generate_xml --s3_input ${S3_INPUT} --s3_output ${S3_OUTPUT}"
if [ "${MOCK}" = "true" ]; then
  cmd="$cmd --mock"
fi

echo "Running command: $cmd"
exec $cmd