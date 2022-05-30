package it.unipi.dii.hadoop.mapreduce;

import it.unipi.dii.hadoop.model.BloomFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.ArrayList;

public class ParameterValidation {
    private static final Log LOG = LogFactory.getLog(ParameterValidation.class);

    public static class ParameterValidationMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        ArrayList<BloomFilter> bloomFilters = new ArrayList<BloomFilter>();
        private int[] counter = new int[11];

        public void setup(Context context) throws IOException, InterruptedException {
            BloomFilter tmp = new BloomFilter();

            for (int i = 0; i <= 10; i++) {
                Path inputFilePath = new Path(context.getConfiguration().get("validation.input")
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

    private static int[] readFalsePositive(Configuration conf, String pathString)
            throws IOException, FileNotFoundException {
        int[] falsePositiveCounter = new int[11];
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] status = hdfs.listStatus(new Path(pathString));

        for (int i = 0; i < status.length; i++) {
            //Read the falsePositive from the hdfs
            if(!status[i].getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));

                br.lines().forEach(
                        (line)->{
                            String[] keyValueSplit = line.split("\t");
                            int rating = Integer.parseInt(keyValueSplit[0]);
                            int nFalsePositive = Integer.parseInt(keyValueSplit[1]);
                            falsePositiveCounter[rating] = nFalsePositive;
                        }
                );
            br.close();
            }
        }

        //Delete temp directory
        //hdfs.delete(new Path(pathString), true);

        return falsePositiveCounter;
    }

    private static int[] readSizes(Configuration conf, String pathString)
            throws IOException, FileNotFoundException {
        int[] sizes = new int[11];
        FileSystem hdfs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(pathString))));

        br.lines().forEach(
                (line) -> {
                    String[] keyValueSplit = line.split("\t");
                    int rating = Integer.parseInt(keyValueSplit[0]);
                    int size = Integer.parseInt(keyValueSplit[1]);
                    sizes[rating] = size;
                }
        );
        br.close();

        return sizes;
    }

    private static void finalize(Configuration conf, double[] falsePositiveRate, String output)
            throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream dos = hdfs.create(new Path(output + "/false-positive.txt"), true);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));

        //Write the result in a unique file
        for(int i = 0; i < falsePositiveRate.length; i++) {
            br.write(String.valueOf(falsePositiveRate[i]));
            br.newLine();
        }

        br.close();
        hdfs.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: ParameterValidation <input> <output>");
            System.exit(1);
        }
        System.out.println("args[0]: <input>="  + otherArgs[0]);
        System.out.println("args[1]: <input>="  + otherArgs[1]);
        System.out.println("args[2]: <output>=" + otherArgs[2]);

        Job job = Job.getInstance(conf, "ParameterValidation");

        // Input Parameter
        job.getConfiguration().set("validation.input", args[1]);

        // Output Parameter
        String validationOutput = args[1] + Path.SEPARATOR + "validation";
        job.getConfiguration().set("validation.output", validationOutput);

        job.setJarByClass(ParameterValidation.class);
        job.setMapperClass(ParameterValidation.ParameterValidationMapper.class);
        job.setReducerClass(ParameterValidation.ParameterValidationReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        if(!job.waitForCompletion(true)) {
            System.exit(1);
        }

        int[] falsePositiveCounter = readFalsePositive(conf, otherArgs[2]);
        int[] sizes = readSizes(conf, "sizes.txt");
        double[] falsePositiveRate = new double[11];
        int tot = 0;

        for (int i=0; i<=10; i++)
            tot += sizes[i];

        for (int i=0; i<=10; i++) {
            falsePositiveRate[i] = (double) 100*falsePositiveCounter[i]/(tot-sizes[i]);
        }

        finalize(conf, falsePositiveRate, otherArgs[2]);

    }
}
