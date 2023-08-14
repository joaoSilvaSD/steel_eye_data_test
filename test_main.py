import unittest
import main

class TestModule1(unittest.TestCase):

    def test_functionality(self):
        result = main.download_xml_file
        self.assertEqual(result, "nice")
