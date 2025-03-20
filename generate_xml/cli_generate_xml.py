import os
import argparse
import boto3
import urllib.parse

from generate_xml.generate_xml_logic1 import generate_xml_data
#from generate_xml.generate_xml_logic2 import generate_xml_data

def copy_from_s3(s3_path: str, local_path: str):
    """
    Copies all objects from the given S3 path to the local directory (recursively).
    """
    parsed = urllib.parse.urlparse(s3_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip('/')
    
    s3 = boto3.client("s3")
    
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response.get("Contents", []):
        key = obj["Key"]
        # Create a relative file path by stripping the prefix
        relative_path = os.path.relpath(key, prefix)
        local_file_path = os.path.join(local_path, relative_path)
        local_dir = os.path.dirname(local_file_path)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        print(f"Downloading s3://{bucket}/{key} to {local_file_path}")
        s3.download_file(bucket, key, local_file_path)

def copy_to_s3(local_folder: str, s3_path: str):
    """
    Uploads all files from local_folder to the destination S3 path (recursively).
    """
    parsed = urllib.parse.urlparse(s3_path)
    bucket = parsed.netloc
    key_prefix = parsed.path.lstrip('/')
    
    s3 = boto3.client("s3")
    
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_folder)
            s3_key = os.path.join(key_prefix, relative_path).replace("\\", "/")
            print(f"Uploading {local_file_path} to s3://{bucket}/{s3_key}")
            s3.upload_file(local_file_path, bucket, s3_key)

def main():
    # Default values from environment variables
    default_s3_input = os.getenv("S3_INPUT")
    default_s3_output = os.getenv("S3_OUTPUT")
    default_mock = os.getenv("MOCK", "false").lower() == "true"
    
    parser = argparse.ArgumentParser("Generate XML from Parquet files using S3 input/output.")
    parser.add_argument("--s3_input", default=default_s3_input, help="S3 URI for the input folder (e.g., s3://mybucket/input)")
    parser.add_argument("--s3_output", default=default_s3_output, help="S3 URI for the output XML files (e.g., s3://mybucket/output)")
    parser.add_argument("--mock", action="store_true", default=default_mock, help="Run in mock mode")
    args = parser.parse_args()
    
    # Copy the S3 input folder to local app/input folder
    local_input = "/app/input"
    local_output = "/app/output"
    print("Copying input data from S3...")
    copy_from_s3(args.s3_input, local_input)
    
    # Ensure output folder exists
    if not os.path.exists(local_output):
        os.makedirs(local_output)
    
    output_file = os.path.join(local_output, "lnrs_output.xml")
    print(f"Generating XML for input folder {local_input} into {output_file}")
    generate_xml_data(
        data_dir=local_input,
        output_file=output_file,
        xsd_file_path="/app/schema.xsd",
        validate_output_xml=False,
        mock=args.mock
    )
    
    # Copy the generated XML files from app/output to the S3 output location
    print("Uploading generated XML files to S3...")
    copy_to_s3(local_output, args.s3_output)

if __name__ == "__main__":
    main()