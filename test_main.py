import unittest
import main

class TestMain(unittest.TestCase):

    def test_download_xml_file(self):
        result = main.download_xml_file(main.XML_URL)
        self.assertNotEqual(result, None)

    def test_read_xml_file(self):
        result = main.read_xml_file(main.XML_URL)

    def test_transform_first_xml(self):
        result = main.transform_first_xml(main.XML_URL)

    def test_download_zip(self):
        result = main.download_zip(main.XML_URL)

    def test_extract_xml_from_zip(self):
        result = main.extract_xml_from_zip(main.XML_URL)

    def test_get_dltins_filename(self):
        result = main.get_dltins_filename(main.XML_URL)

    def test_transform_xml_to_csv(self):
        result = main.transform_xml_to_csv(main.XML_URL)


if __name__ == "__main__":
    unittest.main()



