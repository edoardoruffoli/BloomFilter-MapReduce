package it.unipi.dii.hadoop.mapreduce;

import it.unipi.dii.hadoop.model.BloomFilter;
import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;
import java.util.ArrayList;

public class BloomFilterCreation {

    public static class BloomFilterCreationMapper extends Mapper<Object, Text, IntWritable, BloomFilter> {
        ArrayList<BloomFilter> bloomFilters = new ArrayList<>();
        private int roundRating;

        public void setup(Context context) {
            int m, k;

            for (int i = 0; i <= 10; i++) {
                m = Integer.parseInt(context.getConfiguration().get("input.filter_" + i + ".m"));
                k = Integer.parseInt(context.getConfiguration().get("input.filter_" + i + ".k"));
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

        public void reduce(IntWritable key, Iterable<BloomFilter> values, Context context) throws IOException {
            result = new BloomFilter(values.iterator().next());
            while(values.iterator().hasNext()) {
                 result.or(values.iterator().next().getBitset());
            }

            // Save the final Bloom Filter in the file system
            Path outputFilePath = new Path(context.getConfiguration().get("output.bloom-filters")
                    + Path.SEPARATOR + "filter"
                    + key.toString());
            FileSystem fs = FileSystem.get(context.getConfiguration());

            try (FSDataOutputStream fsdos = fs.create(outputFilePath)) {
                result.write(fsdos);

            } catch (Exception e) {
                throw new IOException("Error while writing bloom filter to file system.", e);
            }
        }
    }

    public static boolean main(Job job) throws Exception {
        Configuration conf = job.getConfiguration();

        job.setJarByClass(BloomFilterCreation.class);

        job.setMapperClass(BloomFilterCreationMapper.class);
        job.setReducerClass(BloomFilterOrReducer.class);

        job.setNumReduceTasks(conf.getInt("job1-n-reducers", 1));

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(BloomFilter.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Input and Output path files
        FileInputFormat.addInputPath(job, new Path(conf.get("input.dataset")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output.bloom-filters")));

        /*job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 800000);*/

        return job.waitForCompletion(conf.getBoolean("verbose", true));
    }
}
