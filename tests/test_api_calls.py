# import pytest
# import requests_mock
# from scripts.algorand_script import fetch_block
# from configs.constants import ALGORAND_ENDPOINT
# import time

# # To run unit tests enter "python -m pytest"


# @pytest.fixture
# def mock_requests_get(requests_mock):
#     # design mock request here
#     def mock_response(block_number, status_code, json_data, next_code = 200):
#         url = f"{ALGORAND_ENDPOINT}/v2/blocks/{block_number}"
#         if status_code == 429:
          
#             wait_time = 0  # Example wait time in seconds
#             requests_mock.get(url, status_code=status_code, json=None,
#                             headers={"Retry-After": str(wait_time)})
#             time.sleep(wait_time)  # Wait before retrying the request
#             requests_mock.get(url, status_code=next_code, json=json_data)        

        
#         requests_mock.get(url, status_code=status_code, json=json_data)

#     return mock_response

# def test_fetch_block(mock_requests_get):


#     # Mock a successful API response with status code 200 and JSON data
#     mock_requests_get(12345, 200, {'key': 'value'})
#     result = fetch_block(12345, {})
#     assert result == {'key': 'value'}

#     # Mock a failed API response with status code 404
#     mock_requests_get(54321, 404, None)
#     result = fetch_block(54321, {})
#     assert result is None

#     # Mock a failed API response with status code 429 (then 200)
#     mock_requests_get(54321, 429, {'key': 'value'})
#     result = fetch_block(54321, {})
#     assert result == {'key': 'value'}

#     # # Mock a failed API response with status code 429 (stuck)
#     # mock_requests_get(54321, 429, None, 429)
#     # result = fetch_block(54321, {})
#     # assert result == None


