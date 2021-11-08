import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class MaxTemperatureTest {

    @Test
    public void run() throws Exception {

        Configuration conf = new Configuration();

        conf.set("mapred.job.tracker", "local");
        conf.set("fs.default.name", "file:///");

        Path inputPath = new Path("data");
        Path outputPath = new Path("output");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = runJob(conf, inputPath, outputPath);
        assertTrue(job.isSuccessful());
    }

    public Job runJob(Configuration conf, Path inputPath, Path outputPath)
            throws ClassNotFoundException, IOException, InterruptedException {
        Job job = Job.getInstance(conf, "max temperature");

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setCombinerClass(MaxTemperatureReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VIntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VIntWritable.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(false);
        return job;
    }
}