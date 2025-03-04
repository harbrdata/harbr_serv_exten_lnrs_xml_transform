import argparse
import time

from generate_xml.generate_xml_logic import generate_xml_data


def get_argparse():
    parser = argparse.ArgumentParser("Generate XML from Parquet files.")
    parser.add_argument(
        "-d", "--data-directory", help="Path to the directory containing Parquet files", required=True
    )
    parser.add_argument(
        "-s", "--schema", help="Path to the XSD schema file", default="schema.xsd", required=True
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
    return parser.parse_args()


def cli_generate_xml():
    start = time.time()
    parser = get_argparse()
    generate_xml_data(data_dir=parser.data_directory, xsd_file_path=parser.schema, output_file=parser.output, validate_output_xml=parser.validate, mock=parser.mock)
    if parser.time:
        print(f"Time taken: {time.time() - start}s")


if __name__ == "__main__":
    cli_generate_xml()
