package it.unipi.dii.cloudcomputing.mapreduce;

import it.unipi.dii.cloudcomputing.BloomFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;

public class ParameterValidation {
    public static class ParameterValidationMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        ArrayList<BloomFilter> bloomFilters = new ArrayList<BloomFilter>();

        public void setup(Context context) throws IOException {
            BloomFilter tmp = new BloomFilter();

            for (int i = 0; i <= 10; i++) {

                Path inputFilePath = new Path(context.getConfiguration().get("validation.input")
                        + "/filter" + i);
                FileSystem fs = FileSystem.get(context.getConfiguration());

                try (FSDataInputStream fsdis = fs.open(inputFilePath)) {
                    tmp.readFields((DataInput) fsdis.getWrappedStream());
                    bloomFilters.add(i, tmp);

                } catch (Exception e) {
                    System.out.println(inputFilePath.toString());
                    throw new IOException("Error while reading bloom filter from file system.", e);
                }
            }

            System.out.println(bloomFilters.get(3).toString());
        }


    }

    public static class ParameterValidationReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {


    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: ParameterValidation <input> <output>");
            System.exit(1);
        }
        System.out.println("args[0]: <input>="  + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);

        Job job = Job.getInstance(conf, "ParameterValidation");

        // Input Parameter
        job.getConfiguration().set("validation.input", args[0]);

        // Output Parameter
        String validationOutput = args[1] + Path.SEPARATOR + "validation";
        job.getConfiguration().set("validation.output", validationOutput);

        job.setJarByClass(ParameterValidation.class);
        job.setMapperClass(ParameterValidation.ParameterValidationMapper.class);
        //job.setReducerClass(BloomFilterCreation.BloomFilterOrReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
