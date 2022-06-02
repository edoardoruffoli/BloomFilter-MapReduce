package it.unipi.dii.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ParameterCalibration {
    public static class CountByRatingMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
        private int roundRating;
        private int[] counter = new int[11];

        public void map(Object key, Text value, Context context) {
            roundRating = (int) Math.round(Double.parseDouble(value.toString().split("\t")[1]));
            counter[roundRating]++;
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (int i=0; i<counter.length; i++)
                context.write(new IntWritable(i), new IntWritable(counter[i]));
        }
    }

    public static class CountSumReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
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

    public static boolean main(Job job) throws Exception {
        Configuration conf = job.getConfiguration();

        job.setJarByClass(ParameterCalibration.class);

        job.setMapperClass(CountByRatingMapper.class);
        job.setReducerClass(CountSumReducer.class);

        job.setNumReduceTasks(conf.getInt("job0-n-reducers", 1));

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // Input and Output path files
        FileInputFormat.addInputPath(job, new Path(conf.get("input.dataset")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output.parameter-calibration")));

        return job.waitForCompletion(conf.getBoolean("verbose", true));
    }
}