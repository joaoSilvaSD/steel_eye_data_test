import unittest
import main
import os
import zipfile
import tempfile
import shutil
import pandas as pd
from unittest.mock import patch, MagicMock, mock_open

class TestMain(unittest.TestCase):

    @patch("main.requests.get")
    def test_failed_download_xml_file(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        result = main.download_xml_file(main.XML_URL)

        self.assertEqual(mock_response.status_code, 404)
        self.assertEqual(result, None)
        mock_get.assert_called_once_with(main.XML_URL)


    @patch('main.requests.get', side_effect=Exception("Mocked exception"))
    def test_exception_download_xml_file(self,mock_get):
        result = main.download_xml_file(main.XML_URL)

        self.assertIsNone(result)
        mock_get.assert_called_once_with(main.XML_URL)


    @patch("main.requests.get")
    def test_successful_download_xml_file(self, mock_get):
        xml_content = b"<xml>content</xml>"

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = xml_content
        mock_get.return_value = mock_response

        response = main.download_xml_file(main.XML_URL)

        self.assertEqual(response, xml_content)
        self.assertEqual(mock_response.status_code, 200)
        mock_get.assert_called_once_with(main.XML_URL)


    @patch('builtins.open', side_effect=Exception("Mocked exception"))
    def test_exception_read_xml_file(self, mock_open):
        xml_path = 'test.xml'
        result = main.read_xml_file(xml_path)
        
        self.assertIsNone(result)
        mock_open.assert_called_once_with(xml_path, 'r', encoding='utf-8')


    @patch('builtins.open', new_callable=mock_open, read_data='<root><item>value</item></root>')
    @patch('xmltodict.parse')
    def test_successful_read_xml_file(self, mock_xmltodict_parse, mock_open):
        expected_result = {'root': {'item': 'value'}}
        
        mock_xmltodict_parse.return_value = expected_result
        
        result = main.read_xml_file(main.XML_LOCAL_NAME)
        
        self.assertEqual(result, expected_result)
        mock_open.assert_called_once_with(main.XML_LOCAL_NAME, 'r', encoding='utf-8')
        mock_xmltodict_parse.assert_called_once()


    def test_sucessful_transform_xml_to_csv(self):
            input_xml_dict = {
                'response': {
                    'result': {
                        'doc': [
                            {
                                'str': [
                                    {'@name': 'checksum', '#text': 'checksum_value'},
                                    {'@name': 'download_link', '#text': 'download_link_value'},
                                    {'@name': 'id', '#text': 'id_value'},
                                    {'@name': 'published_instrument_file_id', '#text': 'file_id_value'},
                                    {'@name': 'file_name', '#text': 'file_name_value'},
                                    {'@name': 'file_type', '#text': 'file_type_value'}
                                ],
                                'date': [
                                    {'@name': 'publication_date', '#text': '2023-08-16'},
                                    {'@name': 'timestamp', '#text': '2023-08-16T12:00:00'}
                                ]
                            }
                        ]
                    }
                }
            }
            
            expected_df = pd.DataFrame([
                {
                    'checksum': 'checksum_value',
                    'download_link': 'download_link_value',
                    'publication_date': '2023-08-16',
                    'id': 'id_value',
                    'published_instrument_file_id': 'file_id_value',
                    'file_name': 'file_name_value',
                    'file_type': 'file_type_value',
                    'timestamp': '2023-08-16T12:00:00'
                }
            ])

            with patch('main.logger') as mock_logger:
                result_df = main.transform_first_xml(input_xml_dict)

                pd.testing.assert_frame_equal(result_df, expected_df)
                mock_logger.info.assert_called_once_with("First xml tranformed to a DataFrame.")


    def test_failed_transform_xml_to_csv(self):
        input_xml_dict = {
            'response': {
                'result': {
                    'doc': []
                }
            }
        }

        with patch('main.logger') as mock_logger:
            result = main.transform_first_xml(input_xml_dict)

            self.assertIsNone(result)
            mock_logger.error.assert_called_once_with("Error transforming dictionary into DataFrame.")


    @patch('main.logger', autospec=True)
    @patch('os.listdir')
    def test_get_dltins_filename(self, mock_listdir, mock_logger):
        mock_listdir.return_value = ["DLTINS_2023-08-15.xml", "other_file.txt", "DLTINS_2023-08-16.xml"]
        filename = main.get_dltins_filename()
        
        self.assertEqual(filename, "DLTINS_2023-08-15.xml")
        mock_logger.info.assert_called_once_with("Filename found: %s.", "DLTINS_2023-08-15.xml")


    @patch('main.logger', autospec=True)
    @patch('os.listdir')
    def test_failed_get_dltins_filename(self, mock_listdir, mock_logger):
        mock_listdir.return_value = ["other_file.txt", "example.csv"]
        filename = main.get_dltins_filename()
        
        self.assertEqual(filename, None)
        mock_logger.error.assert_called_once_with("Could not find file.")


    @patch('main.boto3.client')
    @patch('main.logger', autospec=True)
    def test_transform_xml_to_csv(self, mock_logger, mock_boto3_client):
        sample_xml_dict = {
            'BizData': {
                'Pyld': {
                    'Document': {
                        'FinInstrmRptgRefDataDltaRpt': {
                            'FinInstrm': [
                                {
                                    'TermntdRcrd': {
                                        'FinInstrmGnlAttrbts': {
                                            'Id': 'ID123',
                                            'FullNm': 'Instrument A',
                                            'ClssfctnTp': 'Type A',
                                            'CmmdtyDerivInd': 'No',
                                            'NtnlCcy': 'USD',
                                            'Issr': 'Issuer A'
                                        }
                                    }
                                },
                                # Add more instrument entries here
                            ]
                        }
                    }
                }
            }
        }

        # Mock the S3 client and its put_object method
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Call the function with the sample XML dict
        main.transform_xml_to_csv(sample_xml_dict)
        
        # Assert that the logger was called
        mock_logger.info.assert_called_once_with("Transformed xml into csv.")
        
        # Assert that the S3 client and its put_object method were called
        mock_s3_client.put_object.assert_called_once()
        expected_bucket_name = "joao-silva-csv-bucket"
        expected_csv_file_name = "output.csv"
        mock_s3_client.put_object.assert_called_with(
            Bucket=expected_bucket_name,
            Key=expected_csv_file_name,
            Body=mock_s3_client.put_object.call_args[1]['Body']
        )


    @patch('main.extract_xml_from_zip')
    @patch('main.logger', autospec=True)
    @patch('main.requests.get')
    def test_successful_download_and_extract(self, mock_get, mock_logger, mock_extract_xml):
        # Mock the requests.get response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'zip_content'
        mock_get.return_value = mock_response
        
        # Create a sample DataFrame
        sample_df = pd.DataFrame([
            {'file_type': 'DLTINS', 'download_link': 'https://example.com/file.zip', 'file_name': 'test.zip'}
        ])
        
        # Call the function with the sample DataFrame
        result = main.download_zip(sample_df)
        
        # Assert that the logger was called
        mock_logger.info.assert_called_once_with("%s downloaded successfully.", "test.zip")
        mock_logger.error.assert_not_called()
        
        # Assert that the extract_xml_from_zip function was called
        mock_extract_xml.assert_called_once_with(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test.zip'))
        
        # Assert the response content
        self.assertEqual(result, b'zip_content')
        

    @patch('main.logger', autospec=True)
    @patch('main.requests.get')
    def test_failed_download(self, mock_get, mock_logger):
        # Mock the requests.get response
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        # Create a sample DataFrame
        sample_df = pd.DataFrame([
            {'file_type': 'DLTINS', 'download_link': 'https://example.com/file.zip', 'file_name': 'test.zip'}
        ])
        
        # Call the function with the sample DataFrame
        result = main.download_zip(sample_df)
        
        # Assert that the logger was called
        mock_logger.error.assert_called_once_with("Failed to download the file. Status code: %d", 404)
        mock_logger.info.assert_not_called()
        
        # Assert the returned result
        self.assertIsNone(result)




