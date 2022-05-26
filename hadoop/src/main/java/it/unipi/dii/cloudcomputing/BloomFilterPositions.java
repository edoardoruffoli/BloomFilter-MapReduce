package it.unipi.dii.cloudcomputing;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.Hash;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hadoop.util.hash.Hash.MURMUR_HASH;

public class BloomFilterPositions implements Writable, Comparable<BloomFilterPositions> {
    private int length;
    private int kHash;
    private Set<Integer> bitset;
    private static final int hashType = MURMUR_HASH;

    public BloomFilterPositions(){}

    public BloomFilterPositions(int length, int kHash){
        bitset = new HashSet<>();
        this.length = length;
        this.kHash = kHash;
    }

    public BloomFilterPositions(BloomFilterPositions bf){
        this.bitset = bf.bitset;
        this.length = bf.length;
        this.kHash = bf.kHash;

    }

    public void add(String id){
        //if id is not null
        int seed = 0;
        for (int i = 0; i < kHash; i++){
            seed = Hash.getInstance(hashType).hash(id.getBytes(StandardCharsets.UTF_8), seed);
            bitset.add(Math.abs(seed % length));
        }
    }

    public void or(Set<Integer> input){
        bitset.addAll(input);
    }

    public boolean find(String id){
        //if id is not null
        int seed = 0;
        for (int i = 0; i < kHash; i++){
            seed = Hash.getInstance(hashType).hash(id.getBytes(StandardCharsets.UTF_8), seed);
            if(!bitset.contains(Math.abs(seed % length)))
                return false;
        }
        return true;
    }

    public static BloomFilterPositions copy(final BloomFilterPositions bf){
        return new BloomFilterPositions(bf.length, bf.kHash);
    }

    public Set<Integer> getBitset() {
        return bitset;
    }

    public void setBitset(Set<Integer> bitset) {
        this.bitset = bitset;
    }

    public int getkHash() {
        return kHash;
    }

    public void setkHash(int kHash) {
        this.kHash = kHash;
    }

    public int getLength() {
        return bitset.size();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BloomFilterPositions that = (BloomFilterPositions) o;
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
    public int compareTo(BloomFilterPositions bf) {
        if (bf.equals(bitset))
            return 1;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.length);
        dataOutput.writeInt(this.kHash);

        // https://stackoverflow.com/questions/18406592/how-to-have-bit-string-in-hadoop
        int[] ints = bitset.stream().mapToInt(Integer::intValue).toArray();
        dataOutput.writeInt(ints.length);
        for (int i = 0; i < ints.length; i++) {
            dataOutput.writeInt(ints[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        length = dataInput.readInt();
        kHash = dataInput.readInt();

        // https://stackoverflow.com/questions/18406592/how-to-have-bit-string-in-hadoop
        int[] ints = new int[dataInput.readInt()];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = dataInput.readInt();
        }

        bitset = Arrays.stream(ints).boxed().collect(Collectors.toSet());
    }

    public static void main(String[] args) throws Exception
    {
        BloomFilterPositions b = new BloomFilterPositions(100,5);
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
        BloomFilterPositions deserialized = new BloomFilterPositions();
        deserialized.readFields(in);

        System.out.println(b.toString());

        System.out.println(deserialized.toString());

        System.out.println(b.find("tt10334"));
        System.out.println(b.find("tt50334"));
    }
}
