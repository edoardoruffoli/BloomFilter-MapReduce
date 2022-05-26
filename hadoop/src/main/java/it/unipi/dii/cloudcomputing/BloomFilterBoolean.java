package it.unipi.dii.cloudcomputing;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.Hash;

import static org.apache.hadoop.util.hash.Hash.MURMUR_HASH;

import java.util.Objects;

public class BloomFilterBoolean implements Writable, Comparable<BloomFilter> {
    private int length;
    private int kHash;
    private boolean[] bitset;
    private static final int hashType = MURMUR_HASH;

    public  BloomFilterBoolean (){}

    public BloomFilterBoolean(int length, int kHash){
        bitset = new boolean[length];
        this.length = length;
        this.kHash = kHash;
    }

    public BloomFilterBoolean(BloomFilterBoolean bf){
        this.bitset = bf.bitset.clone();
        this.length = bf.length;
        this.kHash = bf.kHash;

    }

    public void add(String id){
        //if id is not null
        int seed = 0;
        for (int i = 0; i < kHash; i++){
            seed = Hash.getInstance(hashType).hash(id.getBytes(StandardCharsets.UTF_8), seed);
            bitset[Math.abs(seed % length)] = true;
        }
    }

    public void or(boolean[] input){
        for (int i=0; i<input.length; i++)
            bitset[i] = bitset[i] | input[i];
    }

    public boolean find(String id){
        //if id is not null
        int seed = 0;
        for (int i = 0; i < kHash; i++){
            seed = Hash.getInstance(hashType).hash(id.getBytes(StandardCharsets.UTF_8), seed);
            if(!bitset[Math.abs(seed % length)])
                return false;
        }
        return true;
    }

    public static BloomFilterBoolean copy(final BloomFilterBoolean bf){
        return new BloomFilterBoolean(bf.length, bf.kHash);
    }

    public boolean[] getBitset() {
        return bitset;
    }

    public void setBitset(boolean[] bitset) {
        this.bitset = bitset;
    }

    public int getkHash() {
        return kHash;
    }

    public void setkHash(int kHash) {
        this.kHash = kHash;
    }

    public int getLength() {
        return bitset.length;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BloomFilterBoolean that = (BloomFilterBoolean) o;
        return kHash == that.kHash &&  Objects.equals(bitset, that.bitset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bitset, kHash, Hash.getInstance(hashType));
    }

    @Override
    public String toString() {
        return "BloomFilter{" +
                "bitset=" + Arrays.toString(bitset) +
                ", kHash=" + kHash +
                ", length=" + length +
                '}';
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

        for (int i = 0; i < bitset.length; i++) {
            dataOutput.writeBoolean(bitset[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        length = dataInput.readInt();
        kHash = dataInput.readInt();

        bitset = new boolean[length];
        for (int i = 0; i < bitset.length; i++) {
            bitset[i] = dataInput.readBoolean();
        }
    }

    public static void main(String[] args) throws Exception
    {
        BloomFilterBoolean b = new BloomFilterBoolean(100,5);
        b.add("tt10334");
        b.add("tt14334");
        b.add("tt10354");
        b.add("tt20334");
        b.add("tt14334");
        b.add("tt19334");

        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(byteOutput);
        b.write(out);

        DataInput in = new DataInputStream(new ByteArrayInputStream(byteOutput.toByteArray()));
        BloomFilterBoolean deserialized = new BloomFilterBoolean();
        deserialized.readFields(in);

        System.out.println(b.toString());

        System.out.println(deserialized.toString());

        System.out.println(b.find("tt10334"));
        System.out.println(b.find("tt50334"));
    }
}
