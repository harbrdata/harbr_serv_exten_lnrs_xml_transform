# harbr_serv_exten_lnrs_xml_transform
harbr_serv_exten_lnrs_xml_transform

## Install
```
pip install poetry
poetry install --no-root
```

## How to use
```export S3_INPUT='<s3 input path>'```
```export S3_OUTPUT='<s3 export path>'```
```poetry run generate_xml --s3_input ${S3_INPUT} --s3_output ${S3_OUTPUT}```

## How to build the docker image

```docker build --platform linux/amd64 -t harbr_serv_exten_lnrs_xml_transform .```


## How to run the docker image locally

```
docker run --rm \
  -v ~/.aws:/root/.aws:ro \
  -e AWS_PROFILE=<aws-profile> \
  -e S3_INPUT="s3://your-input-bucket/path/" \
  -e S3_OUTPUT="s3://your-output-bucket/path/" \
  -e MOCK="true" \
  harbr_serv_exten_lnrs_xml_transform
```