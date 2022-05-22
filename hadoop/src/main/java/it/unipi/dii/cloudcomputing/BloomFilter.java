package it.unipi.dii.cloudcomputing;

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

    private static final byte[] bitvalues = new byte[] {
        (byte)0x01,
        (byte)0x02,
        (byte)0x04,
        (byte)0x08,
        (byte)0x10,
        (byte)0x20,
        (byte)0x40,
        (byte)0x80
    };

    public  BloomFilter (){

    }

    public BloomFilter(int length, int kHash){
        bitset = new BitSet(length);
        this.length = length;
        this.kHash = kHash;
    }

    public void add(String id){
        //if id is not null
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
        //if id is not null
        int seed = 0;
        for (int i = 0; i < kHash; i++){
            seed = Hash.getInstance(hashType).hash(id.getBytes(StandardCharsets.UTF_8), seed);
            if(!bitset.get(Math.abs(seed % length)))
                return false;
        }
        return true;
    }

    public static BloomFilter copy(final BloomFilter bf){
        return new BloomFilter(bf.length, bf.kHash);
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
        return "BloomFilter{" +
                "bitset=" + bitset +
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
        byte[] bytes = new byte[getNBytes()];
        for(int i = 0, byteIndex = 0, bitIndex = 0; i < length; i++, bitIndex++) {
            if (bitIndex == 8) {
                bitIndex = 0;
                byteIndex++;
            }
            if (bitIndex == 0) {
                bytes[byteIndex] = 0;
            }
            if (bitset.get(i)) {
                bytes[byteIndex] |= bitvalues[bitIndex];
            }
        }
        dataOutput.write(bytes);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        length = dataInput.readInt();
        kHash = dataInput.readInt();
        bitset = new BitSet(length);
        byte[] bytes = new byte[getNBytes()];
        dataInput.readFully(bytes);
        for(int i = 0, byteIndex = 0, bitIndex = 0; i < length; i++, bitIndex++) {
            if (bitIndex == 8) {
                bitIndex = 0;
                byteIndex++;
            }
            if ((bytes[byteIndex] & bitvalues[bitIndex]) != 0) {
                bitset.set(i);
            }
        }
    }

    private int getNBytes(){
        return (length + 7) / 8;
    }

    public static void main(String[] args) throws Exception
    {
        BloomFilter b = new BloomFilter(100,5);
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
        BloomFilter deserialized = new BloomFilter();
        deserialized.readFields(in);

        System.out.println(b.toString());

        System.out.println(deserialized.toString());

        System.out.println(b.find("tt10334"));
        System.out.println(b.find("tt50334"));
    }
}
