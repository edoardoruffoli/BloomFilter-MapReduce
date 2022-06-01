package it.unipi.dii.hadoop.model;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.Hash;

import static org.apache.hadoop.util.hash.Hash.MURMUR_HASH;

import java.util.Objects;

public class BloomFilter implements Writable, Comparable<BloomFilter> {
    private int length;
    private int kHash;
    private BitSet bitset;
    private static final int hashType = MURMUR_HASH;

    public  BloomFilter (){}

    public BloomFilter(int length, int kHash){
        bitset = new BitSet(length);
        this.length = length;
        this.kHash = kHash;
    }

    public BloomFilter(BloomFilter bf){
        this.bitset = (BitSet) bf.bitset.clone();
        this.length = bf.length;
        this.kHash = bf.kHash;
    }

    public void add(String id){

        int seed = 0;
        for (int i = 0; i < kHash; i++){
            seed = Hash.getInstance(hashType).hash(id.getBytes(StandardCharsets.UTF_8), seed);
            bitset.set(Math.abs(seed % length));
        }
    }

    public void or(BitSet input){
        bitset.or(input);
    }

    public boolean find(String id){
        if (length == 0)
            return false;
        int seed = 0;
        for (int i = 0; i < kHash; i++){
            seed = Hash.getInstance(hashType).hash(id.getBytes(StandardCharsets.UTF_8), seed);
            if(!bitset.get(Math.abs(seed % length)))
                return false;
        }
        return true;
    }

    public BitSet getBitset() {
        return bitset;
    }

    public void setBitset(BitSet bitset) {
        this.bitset = bitset;
    }

    public int getkHash() {
        return kHash;
    }

    public void setkHash(int kHash) {
        this.kHash = kHash;
    }

    public int getLength() {
        return bitset.length();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BloomFilter that = (BloomFilter) o;
        return kHash == that.kHash &&  Objects.equals(bitset, that.bitset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bitset, kHash, Hash.getInstance(hashType));
    }

    @Override
    public String toString() {
        return bitset.toString();
    }

    @Override
    public int compareTo(BloomFilter bf) {
        if (bf.equals(bitset))
            return 1;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.length);
        dataOutput.writeInt(this.kHash);

        // https://stackoverflow.com/questions/18406592/how-to-have-bit-string-in-hadoop
        long[] longs = bitset.toLongArray();
        dataOutput.writeInt(longs.length);
        for (int i = 0; i < longs.length; i++) {
            dataOutput.writeLong(longs[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        length = dataInput.readInt();
        kHash = dataInput.readInt();

        // https://stackoverflow.com/questions/18406592/how-to-have-bit-string-in-hadoop
        long[] longs = new long[dataInput.readInt()];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = dataInput.readLong();
        }

        bitset = BitSet.valueOf(longs);
    }
}
