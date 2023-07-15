import pytest
from Utils.data_extractor import DataExtractor


def test_extract_data():
    extractor = DataExtractor()
    data = extractor.extract_data("Test/asserts/TestData.json")
    extractor.close()
    assert isinstance(data, list), "Data is not a list"

    for item in data:
        assert isinstance(item, dict), "Data contains non-dictionary elements"


def test_extract_data_invalid_file():
    extractor = DataExtractor()
    with pytest.raises(FileNotFoundError):
        extractor.extract_data('non_existing_file.json')
    extractor.close()


def test_extract_data_invalid_json():
    extractor = DataExtractor()
    with pytest.raises(ValueError):
        extractor.extract_data('Test/asserts/invalid_json.json')
    extractor.close()


def test_close():
    # Create a temporary test file
    file_path = "Test/asserts/TestData2.json"
    with open(file_path, "w") as file:
        file.write("test data")

    # Instantiate the DataExtractor and call close()
    extractor = DataExtractor()
    extractor.file = open(file_path)
    extractor.close()

    # Verify if the file is closed
    assert extractor.file.closed, "File is not closed"
