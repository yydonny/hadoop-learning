package yangyd.hdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class SeqFile {

  private static final String[] DATA = {
      "One, two, buckle my shoe",
      "Three, four, shut the door",
      "Five, six, pick up sticks",
      "Seven, eight, lay them straight",
      "Nine, ten, a big fat hen"
  };
  private static final String FILE_URL = "hdfs://node1/user/yangyd/file1";

  public static void readDemo() throws IOException {
    SequenceFile.Reader reader = null;
    Configuration configuration = new Configuration();
    try {
      reader = openForRead(FILE_URL, configuration);
      Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
      Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);
      long position = reader.getPosition();
      while (reader.next(key, value)) {
        String syncSeen = reader.syncSeen() ? "*" : "";
        System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
        position = reader.getPosition(); // beginning of next record
      }
    } finally {
      IOUtils.closeStream(reader);
    }
  }

  public static void writeDemo() throws IOException {
    IntWritable key = new IntWritable();
    Text value = new Text();
    SequenceFile.Writer writer = null;
    try {
      writer = openForWrite(FILE_URL);
      for (int i = 0; i < 100; i++) {
        key.set(100 - i);
        value.set(DATA[i % DATA.length]);
        System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
        writer.append(key, value);
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }

  private static SequenceFile.Reader openForRead(String url, Configuration configuration) throws IOException {
    return new SequenceFile.Reader(configuration, SequenceFile.Reader.file(new Path(url)));
  }

  private static SequenceFile.Writer openForWrite(String targetURL) throws IOException {
    return SequenceFile.createWriter(new Configuration(),
        SequenceFile.Writer.file(new Path(targetURL)),
        SequenceFile.Writer.keyClass(IntWritable.class),
        SequenceFile.Writer.valueClass(Text.class));

  }
}
