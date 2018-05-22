package yangyd.hdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

class Utils {
  private static final int BUFF_SIZE = 1024 * 16;

  static String hdfsURL(String arg) {
    if (!arg.startsWith("hdfs://")) {
      throw new IllegalArgumentException("must use absolute URL that starts with hdfs://");
    }
    return arg;
  }

  static FileSystem mount(String url) throws IOException {
    Configuration configuration = new Configuration();
    return FileSystem.get(URI.create(url), configuration);
  }

  static BufferedInputStream openFile(String path) throws FileNotFoundException {
    return new BufferedInputStream(new FileInputStream(path));
  }

  static void pump(OutputStream dst, InputStream src) throws IOException {
    IOUtils.copyBytes(src, dst, BUFF_SIZE, true);
  }
}
