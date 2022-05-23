package it.unipi.dii.cloudcomputing;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class IntArrayWritable extends ArrayWritable {
    public IntArrayWritable() {
        super(IntWritable.class);
    }

    public IntArrayWritable(int[] ints) {
        super(IntWritable.class);
        IntWritable[] intWritable = new IntWritable[ints.length];
        for (int i = 0; i < ints.length; i++) {
            intWritable[i] = new IntWritable(ints[i]);
        }
        set(intWritable);
    }

    public void increment(int index){
        //IntWritable temp = (IntWritable)super.get()[index];
        //temp.set(temp.get() + 1);

        ((IntWritable)super.get()[index]).set(((IntWritable)super.get()[index]).get() + 1);
    }

    public IntWritable[] get() {
        //return (IntWritable[]) super.get();
        Writable[] temp = super.get();
        IntWritable[] values = new IntWritable[temp.length];
        for (int i = 0; i < temp.length; i++) {
            values[i] = (IntWritable)temp[i];
        }
        return values;
    }

    @Override
    public String toString() {
        IntWritable[] values = get();
        String strings = "";
        for (int i = 0; i < values.length; i++) {
            strings = strings + "	"+ values[i].toString();
        }
        return strings;
    }

    public static void main(String[] args) {
        int[] temp = {10, 2 , 10};
        IntArrayWritable pippo = new IntArrayWritable(temp);
        pippo.increment(1);
        System.out.println(pippo.toString());
    }
}
