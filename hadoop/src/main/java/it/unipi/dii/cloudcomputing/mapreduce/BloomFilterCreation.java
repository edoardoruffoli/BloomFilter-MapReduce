package it.unipi.dii.cloudcomputing.mapreduce;

import it.unipi.dii.cloudcomputing.BloomFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class BloomFilterCreation {
    public static class BloomFilterCreationMapper extends Mapper<Object, Text, IntWritable, BloomFilter> {
        ArrayList<BloomFilter> bloomFilters = new ArrayList<BloomFilter>();
        private int roundRating;

        public void setup(Context context) {
            int m, k;

            for (int i = 0; i <= 10; i++) {
                m = Integer.parseInt(context.getConfiguration().get("m_"+i));
                k = Integer.parseInt(context.getConfiguration().get("k_"+i));
                bloomFilters.add(i, new BloomFilter(m, k));
            }
        }

        public void map(Object key, Text value, Context context) {
            roundRating = (int) Math.round(Double.parseDouble(value.toString().split("\t")[1]));
            bloomFilters.get(roundRating).add(value.toString().split("\t")[0]);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i <= 10; i++) {
                context.write(new IntWritable(i), bloomFilters.get(i));
            }
        }
    }

    public static class BloomFilterOrReducer extends Reducer<IntWritable, BloomFilter, IntWritable, BloomFilter> {
        private BloomFilter result;

        public void reduce(IntWritable key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {
            result = new BloomFilter(values.iterator().next());
            for (BloomFilter val : values) {
                 result.or(val.getBitset());
            }

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MatrixMultiplication <input> <output>");
            System.exit(1);
        }
        System.out.println("args[0]: <input>="  + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);

        Job job = Job.getInstance(conf, "MatrixMultiplication");

        double p = 0.01;

        FileInputStream fis=new FileInputStream("sizes.txt");
        Scanner sc=new Scanner(fis);
        int i = 0;
        while (sc.hasNextLine()){
            String[] values = sc.nextLine().split("\t");
            int n = Integer.parseInt(values[1]);
            int m = (int) Math.round((-n*Math.log(p))/(Math.log(2)*Math.log(2)));
            int k = (int) Math.round((m*Math.log(2))/n);

            job.getConfiguration().set("m_"+i, String.valueOf(m));
            job.getConfiguration().set("k_"+i, String.valueOf(k));
            i++;
        }

        job.setJarByClass(BloomFilterCreation.class);
        job.setMapperClass(BloomFilterCreationMapper.class);
        job.setReducerClass(BloomFilterOrReducer.class);

        job.setNumReduceTasks(3);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BloomFilter.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
