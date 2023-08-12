import requests
import pandas as pd
import xmltodict
import os
import zipfile


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
def read_xml_file() -> pd.DataFrame:
    with open(XML_LOCAL_NAME) as xml_file:
        xml_data = xml_file.read()
    xml_dict = xmltodict.parse(xml_data)

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

def main():
    download_xml_file(XML_URL)
    xml = read_xml_file()
    download_zip(xml)



main()