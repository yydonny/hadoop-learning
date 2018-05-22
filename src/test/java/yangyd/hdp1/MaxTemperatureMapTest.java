package yangyd.hdp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.io.IOException;

@RunWith(BlockJUnit4ClassRunner.class)
public class MaxTemperatureMapTest {

  @Test
  public void test1() throws IOException {
    String src = "0029029720999991901100306004+60450+022267FM-12+0014" +
    //                           ^^^^^^^^
        "99999V0201401N004619999999N0000001N9+01281+99999102201ADDGF108991999999999999999999";
    //                                       ^^^^^^
    String year = "1901";
    int temp = 128;

    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = new MapDriver<>();
    mapDriver.setMapper(new MaxTemperatureMapper());
    mapDriver.withInput(new LongWritable(0), new Text(src));

    // expectation
    mapDriver.withOutput(new Text(year), new IntWritable(temp))
        .runTest();
  }

}
