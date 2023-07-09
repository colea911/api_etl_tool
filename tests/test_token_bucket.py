from helpers.token_bucket import TokenBucket
import time

import pytest

@pytest.mark.parametrize("capacity, fill_rate, seconds", [
    (30, 10, 1),  # Test case 1: Rate = 10 tokens/sec, Capacity = 100 tokens
    (15, 25, 1),    # Test case 2: Rate = 5 tokens/sec, Capacity = 50 tokens
])
def test_token_bucket(capacity, fill_rate, seconds):
    # Initialize the token bucket
    token_bucket = TokenBucket(capacity, fill_rate)

    # Calculate the expected number of tokens available based on the rate and capacity
    
    if capacity > fill_rate:
        expected_tokens = capacity + fill_rate * (seconds)
    else:
        expected_tokens = capacity * (seconds + 1)

    # Perform test operations and assertions
    consumed_tokens = 0
    while token_bucket.consume(1):
        consumed_tokens += 1
    time.sleep(seconds)
    while token_bucket.consume(1):
        consumed_tokens += 1

    print(f"consumed_tokens: {consumed_tokens}\nexpected_tokens: {expected_tokens}")
    # Assert that the consumed tokens match the expected number of tokens available
    
    assert consumed_tokens == expected_tokens
