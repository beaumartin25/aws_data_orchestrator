import unittest
from consumer import otherAttributes, otherAttributesUpdate, convertToFileFormat

class TestYourFunctions(unittest.TestCase):
    def test_otherAttributes(self):
        # Define a sample JSON object
        json_object = {
            "widgetId": "12345",
            "owner": "John",
            "label": "MyWidget",
            "description": "A test widget",
            "otherAttributes": [
                {"name": "color", "value": "blue"},
                {"name": "size", "value": "10"},
            ]
        }

        # Call the function to be tested
        result = otherAttributes(json_object)

        # Define the expected result as a dictionary
        expected_result = {
            'id': {'S': '12345'},
            'owner': {'S': 'John'},
            'label': {'S': 'MyWidget'},
            'description': {'S': 'A test widget'},
            'color': {'S': 'blue'},
            'size': {'S': '10'},
        }

        # Compare the actual result to the expected result
        self.assertEqual(result, expected_result)

    def test_otherAttributesUpdate(self):
        # Define a sample JSON object
        json_object = {
            "owner": "John",
            "label": "",
            "description": "A test widget",
            "otherAttributes": [
                {"name": "color", "value": "blue"},
                {"name": "size", "value": ""},
            ]
        }

        # Call the function to be tested
        result = otherAttributesUpdate(json_object)

        # Define the expected result as a dictionary
        expected_result = {
            'owner': {'Action': 'PUT', 'Value': {'S': 'John'}},
            'label': {'Action': 'DELETE'},
            'description': {'Action': 'PUT', 'Value': {'S': 'A test widget'}},
            'color': {'Action': 'PUT', 'Value': {'S': 'blue'}},
            'size': {'Action': 'DELETE'},
        }

        # Compare the actual result to the expected result
        self.assertEqual(result, expected_result)

    def test_convertToFileFormat(self):

        result = convertToFileFormat("John Doe", "alsdkjfklehljd9183u")
        

        self.assertEqual(f"widgets/john-doe/alsdkjfklehljd9183u", result)

    

if __name__ == '__main__':
    unittest.main()
