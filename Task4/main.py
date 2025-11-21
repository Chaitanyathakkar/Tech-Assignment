class LongestSequenceStream:
    def __init__(self):
        # Map each number to the length of the consecutive sequence it belongs to
        self.map = {}
        self.longest = 0

    def add(self, num):
        if num in self.map:
            return  # Ignore duplicates

        left = self.map.get(num - 1, 0)
        right = self.map.get(num + 1, 0)

        # Total length of new interval
        new_length = left + right + 1
        self.map[num] = new_length

        # Update boundaries
        self.map[num - left] = new_length
        self.map[num + right] = new_length

        # Update global max
        self.longest = max(self.longest, new_length)

    def get_longest(self):
        return self.longest


# ===========================
# Example Usage
# ===========================
stream = LongestSequenceStream()

inputs = [100, 4, 200, 1, 3, 2]

for x in inputs:
    stream.add(x)
    print(f"Added {x}, longest = {stream.get_longest()}")

# Final longest sequence should be 4 (1,2,3,4)
