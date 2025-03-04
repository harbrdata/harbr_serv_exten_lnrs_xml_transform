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

```docker build -t harbr_serv_exten_lnrs_xml_transform .```

## How to run the docker image

```docker run -v $(pwd)/input:/app/input -v $(pwd)/output:/app/output harbr_serv_exten_lnrs_xml_transform -s schema.xsd -o data.xml -t -d input```