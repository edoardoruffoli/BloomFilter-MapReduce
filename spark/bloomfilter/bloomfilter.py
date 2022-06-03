import copy

from bitarray import bitarray
import mmh3


class Bloomfilter:
    def __init__(self, length, kHash):
        self.length = length
        self.kHash = kHash
        self.bits = bitarray(length)

    def copy(self):
        return copy.deepcopy(self)

    def add(self, id):
        seed = 0
        for i in range(self.kHash):
            seed = mmh3.hash(id, seed)
            self.bits[abs(seed % self.length)] = 1

    def _or(self, input):
        self.bits = self.bits | input.bits

    def print(self):
        print(self.bits.tolist())

    def find(self, id):
        seed = 0
        for i in range(self.kHash):
            seed = mmh3.hash(id, seed)
            if self.bits[abs(seed % self.length)] == 0:
                return False
        return True
