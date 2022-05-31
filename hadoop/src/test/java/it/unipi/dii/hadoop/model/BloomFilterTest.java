package it.unipi.dii.hadoop.model;

import junit.framework.TestCase;

import java.io.*;

public class BloomFilterTest extends TestCase {
    private BloomFilter b1 = new BloomFilter(100,5);
    private BloomFilter b2;

    protected void setUp() throws Exception {
        super.setUp();
        b1.add("tt10334");
        b1.add("tt14334");
        b1.add("tt10354");
        b1.add("tt20334");
        b1.add("tt14334");
        b1.add("tt19334");
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testFind1() {
        System.out.println("Find Test 1 value: tt10334");
        assertTrue(b1.find("tt10334"));
    }

    public void testFind2() {
        System.out.println("Find Test 2 value: tt50334");
        assertFalse(b1.find("tt50334"));
    }

    public void testWritable1() {
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(byteOutput);
        BloomFilter deserialized = new BloomFilter();
        try {
            b1.write(out);
            DataInput in = new DataInputStream(new ByteArrayInputStream(byteOutput.toByteArray()));
            deserialized.readFields(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        assertTrue(b1.toString().equals(deserialized.toString()));
    }

}
