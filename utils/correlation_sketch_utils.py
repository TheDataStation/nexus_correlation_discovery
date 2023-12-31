import math
import mmh3
import heapq

"""
The inverse golden ratio as a fraction. This has higher precision than using the formula:
(Math.sqrt(5.0) - 1.0) / 2.0.
"""
INVERSE_GOLDEN_RATIO = 0.6180339887498949025
"""
The golden ratio constant, i.e., (Math.sqrt(5) + 1) / 2.
"""
GOLDEN_RATIO = INVERSE_GOLDEN_RATIO + 1

def murmur3_32(key: str):
    return mmh3.hash(key)

def grm(hash: int):
    h = (hash + 1) * GOLDEN_RATIO
    return h - math.floor(h)

class FixedSizeMaxHeap:
    def __init__(self, max_size) -> None:
        self.max_size = max_size
        self.data = []
    
    def push(self, item):
        item = (-item[0], item[1])
        if len(self.data) < self.max_size:
            heapq.heappush(self.data, item)
        else:
            heapq.heappushpop(self.data, item)
    
    def pop(self):
        item = heapq.heappop(self.data)
        return (-item[0], item[1])

    def get_data(self):
        return [(-item[0], item[1]) for item in self.data]


    