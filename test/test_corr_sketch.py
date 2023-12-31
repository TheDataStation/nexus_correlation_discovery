from utils.correlation_sketch_utils import murmur3_32, grm, FixedSizeMaxHeap

def test_hash_a_key():
    hash_key = murmur3_32('foo')
    val = grm(hash_key)
    print(val)

def test_max_heap():
    max_heap = FixedSizeMaxHeap(3)
    max_heap.push((1, 'a'))
    max_heap.push((2, 'b'))
    max_heap.push((3, 'c'))
    max_heap.push((4, 'd'))
    max_heap.push((5, 'e'))
    print(max_heap.get_data())

if __name__ == "__main__":
    test_max_heap()