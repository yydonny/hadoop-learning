package yangyd.hdemo;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Usage:
 *   hadoop yangyd.hdemo.Upload input/docs/1400-8.txt hdfs://localhost/user/tom/1400-8.txt
 */
public class Upload {
  public static void main(String... args) throws IOException {
    if (args.length < 2) {
      throw new IllegalArgumentException("too few arguments");
    }
    String target = Utils.hdfsURL(args[1]);
    InputStream src = Utils.openFile(args[0]);
    FileSystem fileSystem = Utils.mount(target);

    System.out.print("Uploading ");

    // report progress while transferring
    OutputStream targetOut = fileSystem.create(new Path(target), () -> System.out.print('.'));
    Utils.pump(targetOut, src);
  }
}
