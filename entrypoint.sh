set -e

cmd="poetry run generate_xml -s ${SCHEMA} -o ${OUTPUT_PATH} -t -d ${INPUT_FOLDER}"

if [ "${MOCK}" = "true" ]; then
  cmd="$cmd -m"
fi
echo "Running command: $cmd"
exec $cmd