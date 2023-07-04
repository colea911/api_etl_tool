import requests
import logging
import time


# These Functions do Error Handling for APIs

def handle_response(response, headers, wait_count=0, max_waits = 5):
    
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:  # Rate limit exceeded
            if wait_count < max_waits:
                wait_time = 5  # Default wait time if no Retry-After header
                if "Retry-After" in response.headers:
                    wait_time = int(response.headers["Retry-After"])
                    logging.warning(f"Rate limit exceeded. Waiting for {wait_time} seconds.")
                else:
                    logging.warning("Rate limit exceeded. No Retry-After header found.")
                    max_waits = 2

                time.sleep(wait_time)
                response = requests.get(response.url, headers=headers)
                return handle_response(response, headers, wait_count + 1)
            else:
                logging.error(f"Exceeded maximum number of waits.")
        else:
            logging.error(f"HTTPError: {str(e)}")
            logging.error(f"Response content: {response.content}")

def handle_request_exception(request_id, e):
    if isinstance(e, requests.exceptions.HTTPError):
        logging.error(f"HTTPError for request {request_id}: {str(e)}")
    elif isinstance(e, requests.exceptions.ConnectionError):
        logging.error(f"ConnectionError for request {request_id}: {str(e)}")
    elif isinstance(e, requests.exceptions.Timeout):
        logging.error(f"Timeout for request {request_id}: {str(e)}")
    else:
        logging.error(f"RequestException for request {request_id}: {str(e)}")

