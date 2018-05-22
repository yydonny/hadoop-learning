package yangyd.hdp1;

import org.apache.hadoop.io.Text;

public class NCDCRecordParser {
  private static final int MISSING_TEMPERATURE = 9999;

  private String year;
  private int temperature;
  private String quality;

  public void parse(Text record) {
    parse(record.toString());
  }

  public void parse(String record) {
    year = record.substring(15, 19);
    temperature = Integer.parseInt(record.substring(87, 92));
    quality = record.substring(92, 93);
  }

  public String getYear() {
    return year;
  }

  public int getTemperature() {
    return temperature;
  }

  public String getQuality() {
    return quality;
  }
}
