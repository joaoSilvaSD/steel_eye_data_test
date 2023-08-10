import requests

# URL of the XML file you want to download
XML_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"


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
            with open("downloaded_file.xml", "wb") as f:
                f.write(xml_content)
            print("XML file downloaded successfully.")
        else:
            raise Exception(f"Failed to download XML file. Status code: {response.status_code}")
    except Exception as e:
        raise Exception(f"Error while downloading '{xml_url}': {str(e)}")



def main():
    download_xml_file(XML_URL)

main()