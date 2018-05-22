package yangyd.hdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Load configs from standard locations and print them out.
 */
public class PrintConfig extends Configured implements Tool {
  static {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("yarn-default.xml");
    Configuration.addDefaultResource("yarn-site.xml");
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  @Override
  public int run(String[] args) {
    Configuration configuration = getConf();
    configuration.forEach(entry ->
        System.out.println(entry.getKey() + " -> " + entry.getValue()));
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new PrintConfig(), args);
    System.exit(exitCode);
  }
}
