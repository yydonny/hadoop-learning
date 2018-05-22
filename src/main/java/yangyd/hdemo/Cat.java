package yangyd.hdemo;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Usage:
 *   hadoop yangyd.hdemo.Cat hdfs://localhost/user/tom/quangle.txt
 */
public class Cat {
  public static void main(String... args) throws IOException {
    String url = Utils.hdfsURL(args[0]);
    Utils.pump(System.out, open(url));
  }

  private static InputStream open(String url) throws IOException {
    return new URL(url).openStream();
  }

  private static InputStream open(String url, long start) throws IOException {
    FileSystem fileSystem = Utils.mount(url);
    FSDataInputStream fsdi = fileSystem.open(new Path(url));
    fsdi.seek(start); // random access
    return fsdi;
  }

}
