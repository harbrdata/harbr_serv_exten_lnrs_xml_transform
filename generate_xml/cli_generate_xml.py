import argparse
import os.path
import time

from generate_xml.generate_xml_logic1 import generate_xml_data as generate_xml_data1
from generate_xml.generate_xml_logic2 import generate_xml_data as generate_xml_data2


def get_argparse():
    parser = argparse.ArgumentParser("Generate XML from Parquet files.")
    parser.add_argument(
        "-d", "--data-directory", help="Path to the directory containing Parquet files", required=True
    )
    parser.add_argument(
        "-o", "--output", help="Path to the output XML file", default="output.xml", required=True
    )
    parser.add_argument(
        "-t", "--time", help="Show the time taken to generate the XML", action="store_true"
    )
    parser.add_argument(
        "-v", "--validate", help="Validate the generated XML against the schema", action="store_true"
    )
    parser.add_argument(""
                        "-m", "--mock", help="Create a mock XML file", action="store_true")
    parser.add_argument("-V", "--version", required=False, choices=["1", "2"], default="1", help="Version of generate xml to use: 1 or 2")
    return parser.parse_args()


def cli_generate_xml():
    start = time.time()
    parser = get_argparse()
    schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "schema.xsd")
    version = parser.version
    if version == "1":
        generate_xml_data1(data_dir=parser.data_directory, xsd_file_path=schema_path, output_file=parser.output, validate_output_xml=parser.validate, mock=parser.mock)
    elif version == "2":
        generate_xml_data2(data_dir=parser.data_directory, xsd_file_path=schema_path, output_file=parser.output, validate_output_xml=parser.validate, mock=parser.mock)
    else:
        raise ValueError(f"Invalid version: {version}")
    if parser.time:
        print(f"Time taken: {time.time() - start}s")


if __name__ == "__main__":
    cli_generate_xml()
