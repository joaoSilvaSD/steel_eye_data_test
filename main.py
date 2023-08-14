import requests
import pandas as pd
import xmltodict
import os
import zipfile
import xml.etree.ElementTree as ET
import csv
from io import StringIO
import boto3

# Your AWS S3 bucket and CSV file name
s3_bucket_name = 'your-s3-bucket-name'
csv_file_name = 'output.csv'



# URL of the XML file you want to download
XML_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
XML_LOCAL_NAME = "downloaded_file.xml"


'''
    download the xml file from url if the status code is 200
'''
def download_xml_file(xml_url: str) -> None:
    try:
        # Send an HTTP GET request to the URL
        response = requests.get(xml_url)

        if response.status_code == 200:
            # Get content from response
            xml_content = response.content
            
            # Save the content to a local file
            with open(XML_LOCAL_NAME, "wb") as f:
                f.write(xml_content)
            print("XML file downloaded successfully.")
        else:
            raise Exception(f"Failed to download XML file. Status code: {response.status_code}")
    except Exception as e:
        raise Exception(f"Error while downloading '{xml_url}': {str(e)}")

'''
    read xml file and tranform to a dataFrame
'''
def read_xml_file(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as xml_file:
        xml_data = xml_file.read()
    xml_dict = xmltodict.parse(xml_data)

    return xml_dict
    
def transform_first_xml(xml_dict: dict) ->pd.DataFrame:
    docs = xml_dict['response']['result']['doc']

    data = []
    for doc in docs:
        checksum = next(item['#text'] for item in doc['str'] if item['@name'] == 'checksum')
        download_link = next(item['#text'] for item in doc['str'] if item['@name'] == 'download_link')
        publication_date = next(item['#text'] for item in doc['date'] if item['@name'] == 'publication_date')
        id_value = next(item['#text'] for item in doc['str'] if item['@name'] == 'id')
        published_instrument_file_id = next(item['#text'] for item in doc['str'] if item['@name'] == 'published_instrument_file_id')
        file_name = next(item['#text'] for item in doc['str'] if item['@name'] == 'file_name')
        file_type = next(item['#text'] for item in doc['str'] if item['@name'] == 'file_type')
        timestamp = next(item['#text'] for item in doc['date'] if item['@name'] == 'timestamp')

        data.append({
            'checksum': checksum,
            'download_link': download_link,
            'publication_date': publication_date,
            'id': id_value,
            'published_instrument_file_id': published_instrument_file_id,
            'file_name': file_name,
            'file_type': file_type,
            'timestamp': timestamp
        })

    return pd.DataFrame(data)

def download_zip(df: pd.DataFrame) -> None:
    filtered_df = df[df['file_type'] == 'DLTINS']
    
    if not filtered_df.empty:
        first_item = filtered_df.iloc[0]
        download_zip_link = first_item['download_link']

        if download_zip_link:
            # Save .zip to the directory of the Python script itself
            save_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), first_item['file_name'])

            response = requests.get(download_zip_link)

            if response.status_code == 200:
                with open(save_path, "wb") as file:
                    file.write(response.content)
                print(first_item['file_name'] + " downloaded successfully.")

                extract_xml_from_zip(save_path)
            else:
                 raise Exception("Failed to download the file. Status code:", response.status_code)
        else:
            raise Exception("Failed find download_link in dataFrame.")
    else:
        raise Exception("Failed find DLTINS file_type in dataFrame.")


def extract_xml_from_zip(path: str) -> None:
    # Create the output directory if it doesn't exist
    if not os.path.exists(os.path.dirname(os.path.abspath(__file__))):
        os.makedirs(os.path.dirname(os.path.abspath(__file__)))

    # Extract all files from the zip archive
    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall(os.path.dirname(os.path.abspath(__file__)))

    # Search for XML files in the output directory
    xml_files = [file for file in os.listdir(os.path.dirname(os.path.abspath(__file__))) if file.endswith(".xml")]

    if xml_files:
        xml_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), xml_files[0])
        print("XML file extracted:", xml_file_path)
    else:
        print("No XML files extracted from the zip.")

def get_dltins_filename() -> str:
    # List all files in the directory
    files_in_directory = os.listdir(os.path.dirname(os.path.abspath(__file__)))
    filtered_files = [filename for filename in files_in_directory if filename.startswith("DLTINS_") and filename.endswith(".xml")]

    if filtered_files:
        xml_file_name = filtered_files[0]

    return xml_file_name

def transform_xml_to_csv(xml_dix: dict):
    # Extracting the necessary information from the dictionary
    instruments = xml_dix['BizData']['Pyld']['Document']['FinInstrmRptgRefDataDltaRpt']['FinInstrm']
    # Create a CSV file in memory
    
    csv_output = StringIO()
    csv_writer = csv.writer(csv_output)

    # Write the CSV header
    header = [
        'FinInstrmGnlAttrbts.Id',
        'FinInstrmGnlAttrbts.FullNm',
        'FinInstrmGnlAttrbts.ClssfctnTp',
        'FinInstrmGnlAttrbts.CmmdtyDerivInd',
        'FinInstrmGnlAttrbts.NtnlCcy',
        'Issr'
    ]
    csv_writer.writerow(header)

    # Iterate through instruments and extract data
    for instrm in instruments:
        try:
            instrm_gnl_attrbts = instrm['TermntdRcrd']
            print(instrm_gnl_attrbts)

            id_ = instrm_gnl_attrbts['FinInstrmGnlAttrbts']['Id']
            full_nm = instrm_gnl_attrbts['FinInstrmGnlAttrbts']['FullNm']
            clssfctn_tp = instrm_gnl_attrbts['FinInstrmGnlAttrbts']['ClssfctnTp']
            cmmdty_deriv_ind = instrm_gnl_attrbts['FinInstrmGnlAttrbts']['CmmdtyDerivInd']
            ntnl_ccy = instrm_gnl_attrbts['FinInstrmGnlAttrbts']['NtnlCcy']
            issr = instrm_gnl_attrbts['Issr']

            # Write data to CSV
            csv_writer.writerow([id_, full_nm, clssfctn_tp, cmmdty_deriv_ind, ntnl_ccy, issr])

        except KeyError:
            # Skip this line
            pass

    # Get the CSV content
    csv_content = csv_output.getvalue()

    # Upload the CSV content to S3
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=s3_bucket_name, Key=csv_file_name, Body=csv_content)

    print(f'CSV file has been uploaded to {s3_bucket_name}/{csv_file_name}')


def main():
    download_xml_file(XML_URL)
    xml_dic_1 = read_xml_file(XML_LOCAL_NAME)
    xml_df = transform_first_xml(xml_dic_1)
    download_zip(xml_df)

    dltins_filename = get_dltins_filename()
    xml_dic_2 = read_xml_file(dltins_filename)
    transform_xml_to_csv(xml_dic_2)




main()