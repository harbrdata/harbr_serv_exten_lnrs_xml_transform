# harbr_serv_exten_lnrs_xml_transform
harbr_serv_exten_lnrs_xml_transform

## Install
```
pip install poetry
poetry install --no-root
```

## How to use

```poetry run generate_xml -s schema.xsd -o data.xml -t -d input```

## How to build the docker image

```docker build -t generate_xml .```

## How to run the docker image

```
sudo docker run -d  \
  -v $(pwd)/input:/app/input \
  -v $(pwd)/output:/app/output \
  --env OUTPUT_PATH=/app/output/data.xml \
  --env INPUT_FOLDER=/app/input \
  --env MOCK=true \
  generate_xml
```