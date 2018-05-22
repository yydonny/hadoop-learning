package yangyd.hdp1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
  public static void main(String... args) {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    try {
      Job job = Job.getInstance();
      job.setJarByClass(Main.class);
      job.setJobName("Max temperature");

      // mapper & reducer
      job.setMapperClass(MaxTemperatureMapper.class);
      job.setReducerClass(MaxTemperatureReducer.class);

      // define output format
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      // input and output
      FileInputFormat.addInputPath(job, new Path(args[0])); // input is automatically unzipped

      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

      System.exit(job.waitForCompletion(true) ? 0 : 1);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
