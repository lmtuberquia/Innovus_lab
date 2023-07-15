import json


class DataExtractor:
    """
        DataExtractor is responsible for extracting data from a JSON file.

        Attributes:
            file: File object for the JSON file.

        Methods:
            extract_data(file_path): Extracts data from the JSON file.
            close(): Closes the file object.

        Example Usage:
            extractor = DataExtractor()
            data = extractor.extract_data('data.json')
            extractor.close()
        """
    def __init__(self):
        self.file = None

    def extract_data(self, file_path):
        """
                Extracts data from a JSON file.

                Args:
                    file_path (str): Path to the JSON file.

                Returns:
                    dict: Extracted data as a dictionary.

                Raises:
                    FileNotFoundError: If the specified file is not found.
                    ValueError: If the JSON file has an invalid format.
        """
        try:
            self.file = open(file_path)
            data = json.load(self.file)
            return data
        except FileNotFoundError:
            raise FileNotFoundError(f"File '{file_path}' not found.")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON format in file '{file_path}'.")

    def close(self):
        """
        Closes the file object.
        """
        if self.file:
            self.file.close()
