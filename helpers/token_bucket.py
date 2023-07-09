
import time

class TokenBucket:
    def __init__(self, capacity, fill_rate):
        self.capacity = capacity  # Maximum number of tokens in the bucket
        self.tokens = capacity  # Current number of tokens in the bucket
        self.fill_rate = fill_rate  # Tokens added per second
        self.last_time = time.time()  # Timestamp of the last token consumption
    
    def consume(self, tokens=1):
        current_time = time.time()
        elapsed_time = current_time - self.last_time

        # Refill the bucket if tokens need to be added
        tokens_to_add = elapsed_time * self.fill_rate
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)

        # Check if enough tokens are available
        if tokens <= self.tokens:
            self.tokens -= tokens
            self.last_time = current_time
            return True  # Request is allowed

        # Sleep for the remaining time if rate is exceeded
        time.sleep(1 - (self.tokens / self.fill_rate))
        self.tokens = 0
        self.last_time = time.time()
        return False  # Request is rejected due to rate limit