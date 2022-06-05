import copy

from bitarray import bitarray
import mmh3


class Bloomfilter(object):
    def __init__(self, length, kHash):
        self.length = length
        self.kHash = kHash
        self.bits = bitarray(length)
        self.bits.setall(0)
    def copy(self):
        return copy.deepcopy(self)

    def add(self, id):
        seed = 0
        for i in range(self.kHash):
            seed = mmh3.hash(id, seed)
            self.bits[abs(seed % self.length)] = 1

    def bitwise_or(self, input):
        self.bits = self.bits | input.bits
        return self

    def print(self):
        print(self.bits)

    def find(self, id):
        seed = 0
        for i in range(self.kHash):
            seed = mmh3.hash(id, seed)
            if self.bits[abs(seed % self.length)] == 0:
                return False
        return True


if __name__ == "__main__":
    bf = [Bloomfilter(20, 3)]
