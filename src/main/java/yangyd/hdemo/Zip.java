package yangyd.hdemo;

import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.IOException;

/**
 * Usage:
 *   hadoop yangyd.hdemo.Zip
 * This program reads from stdin and writes to stdout.
 */
public class Zip {
  public static void main(String... args) throws IOException {
    CompressionOutputStream output = new GzipCodec().createOutputStream(System.out);
    Utils.pump(output, System.in);
    output.flush();
  }
}
