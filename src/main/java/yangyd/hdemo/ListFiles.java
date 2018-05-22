package yangyd.hdemo;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Usage:
 *   hadoop yangyd.hdemo.ListFiles hdfs://localhost/user/tom
 */
public class ListFiles {
  public static void main(String... args) throws IOException {
    String url = Utils.hdfsURL(args[0]);
    FileSystem fileSystem = Utils.mount(url);
    FileStatus[] status = fileSystem.listStatus(new Path(url));
    for (Path p : FileUtil.stat2Paths(status)) {
      System.out.println(p);
    }
  }
}
