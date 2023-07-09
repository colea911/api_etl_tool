class Counter:
    def __init__(self):
        self.count = 1

    def __call__(self):
        current_count = self.count
        self.count += 1
        return current_count