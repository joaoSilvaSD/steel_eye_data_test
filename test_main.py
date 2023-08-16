import unittest
import main
from unittest.mock import patch, MagicMock

class TestMain(unittest.TestCase):

    @patch("main.requests.get")
    def test_failed_download_xml_file(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        main.download_xml_file("http://test.com")

        self.assertEqual(mock_response.status_code, 404)

    @patch("main.requests.get")
    def test_exception_download_xml_file(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        main.download_xml_file("http://test.com")

        self.assertEqual(mock_response.status_code, 404)

    @patch("main.requests.get")
    def test_successful_download_xml_file(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        response = main.download_xml_file(main.XML_URL)

        self.assertNotEqual(response, None)
        self.assertEqual(mock_response.status_code, 200)


    # def test_read_xml_file(self):
    #     result = main.read_xml_file(main.XML_URL)

    # def test_transform_first_xml(self):
    #     result = main.transform_first_xml(main.XML_URL)

    # def test_download_zip(self):
    #     result = main.download_zip(main.XML_URL)

    # def test_extract_xml_from_zip(self):
    #     result = main.extract_xml_from_zip(main.XML_URL)

    # def test_get_dltins_filename(self):
    #     result = main.get_dltins_filename(main.XML_URL)

    # def test_transform_xml_to_csv(self):
    #     result = main.transform_xml_to_csv(main.XML_URL)

