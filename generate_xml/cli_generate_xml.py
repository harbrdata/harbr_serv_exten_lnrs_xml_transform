import os
import argparse
import boto3
import urllib.parse
import pyzipper
import json
from datetime import datetime

from generate_xml.generate_xml_logic1 import generate_xml_data
#from generate_xml.generate_xml_logic2 import generate_xml_data

def _get_ssm_secret(secret_name: str) -> str:
    """
    Retrieve a secure string from SSM Parameter Store
    """
    ssm = boto3.client("ssm")
    resp = ssm.get_parameter(Name=secret_name, WithDecryption=True)
    return resp["Parameter"]["Value"]

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

def copy_to_s3(local_file: str, s3_folder: str):
    """
    Upload a single local file to "s3_folder"
    """
    parsed = urllib.parse.urlparse(s3_folder)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip('/')

    if not prefix.endswith('/'):
        prefix += '/'
    key = f"{prefix}{os.path.basename(local_file)}"
    boto3.client("s3").upload_file(local_file, bucket, key)

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

    # Read zip parameters from organization_details.json
    org_json = os.path.join(local_input, "organization_details.json")
    with open(org_json, "r") as jf:
        org = json.load(jf)

    code = org["client_three_letter_code"]
    cut = org["product_cut"]
    zip_secret = org["client_zip_remote_secret"]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if cut == "04":
        label = "US_IRS_FATCA_FF"
    else:
        label = "WorldCompliancePlus"

    output_folder = f"{code}{cut}XF_{label}_{ts}"
    zip_password  = _get_ssm_secret(zip_secret)
    
    # Ensure output folder exists
    if not os.path.exists(local_output):
        os.makedirs(local_output)
    
    output_file = os.path.join(local_output, "Entities.xml")
    print(f"Generating XML for input folder {local_input} into {output_file}")
    generate_xml_data(
        data_dir=local_input,
        output_file=output_file,
        xsd_file_path="/app/schema.xsd",
        validate_output_xml=False,
        mock=args.mock
    )
    
    # ZIP the Entities.xml with AES-256 + password
    xml_file = os.path.join(local_output, "Entities.xml")
    zip_name = f"{output_folder}.zip"
    zip_path = os.path.join(local_output, zip_name)
    print(f"Creating ZIP file: {zip_path}")
    with pyzipper.AESZipFile(
        zip_path, 'w',
        compression=pyzipper.ZIP_DEFLATED,
        encryption=pyzipper.WZ_AES
    ) as zf:
        zf.setpassword(zip_password.encode())
        zf.setencryption(pyzipper.WZ_AES, nbits=256)
        # Store the XML at the root of the zip
        print(f"Writing XML file {xml_file} to {zip_path}")
        zf.write(xml_file, arcname="Entities.xml")
        print("Completed ZIP operations")
    
    # Upload the ZIP to S3
    print(f"Uploading encrypted ZIP {zip_name} to {args.s3_output}")
    copy_to_s3(zip_path, args.s3_output)
    print("Transformation job completed")

if __name__ == "__main__":
    main()