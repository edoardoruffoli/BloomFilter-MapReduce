package it.unipi.dii.hadoop.mapreduce;

import it.unipi.dii.hadoop.model.BloomFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.ArrayList;

public class ParameterValidation {

    public static class ParameterValidationMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        ArrayList<BloomFilter> bloomFilters = new ArrayList<BloomFilter>();
        private int[] counter = new int[11];

        public void setup(Context context) throws IOException, InterruptedException {
            BloomFilter tmp = new BloomFilter();

            for (int i = 0; i <= 10; i++) {
                Path inputFilePath = new Path(context.getConfiguration().get("output.bloomfilter")
                        + "/filter" + i);
                FileSystem fs = FileSystem.get(context.getConfiguration());

                try (FSDataInputStream fsdis = fs.open(inputFilePath)) {
                    tmp.readFields(fsdis);
                    bloomFilters.add(i, tmp);

                } catch (Exception e) {
                    System.out.println(inputFilePath.toString());
                    throw new IOException("Error while reading bloom filter from file system.", e);
                }
            }
        }

        public void map(Object key, Text value, Context context) {
            double rating = Double.parseDouble(value.toString().split("\t")[1]);
            int roundRating = (int) Math.round(rating);

            for (int i=0; i<=10; i++) {
                if (i == roundRating)
                    continue;
                if (bloomFilters.get(i).find(value.toString().split("\t")[0]))
                    counter[i]++;
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i <= 10; i++) {
                context.write(new IntWritable(i), new IntWritable(counter[i]));
            }
        }
    }

    public static class ParameterValidationReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static boolean main(Job job) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = job.getConfiguration();

        job.setJarByClass(ParameterValidation.class);
        job.setMapperClass(ParameterValidation.ParameterValidationMapper.class);
        job.setReducerClass(ParameterValidation.ParameterValidationReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(conf.get("input.dataset")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output.false-positive-count")));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(conf.getBoolean("verbose", true));
    }
}
