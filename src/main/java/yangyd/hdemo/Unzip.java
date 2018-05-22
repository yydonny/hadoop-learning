package yangyd.hdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Unzip {

  public static void main(String... args) throws IOException {
    String url = Utils.hdfsURL(args[0]);
    FileSystem fileSystem = Utils.mount(url);

    CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
    Path path = new Path(url);
    CompressionCodec codec = factory.getCodec(path);

    if (codec == null) {
      System.err.println("not supported: " + url);
      System.exit(1);
    }

    String outFile = CompressionCodecFactory.removeSuffix(url, codec.getDefaultExtension());

    try (InputStream in = codec.createInputStream(fileSystem.open(path));
        OutputStream out = fileSystem.create(path))
    {
      Utils.pump(out, in);
    }
  }
}
