import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MaxTemperatureMapper
        extends Mapper<Object, Text, Text, VIntWritable> {

    static final int INVALID_TEMPERATURE_VALUE = 9999;

    Set<Integer> qualityFlags = new HashSet<Integer>() {{
        add(0); add(1); add(4); add(5); add(9);
    }};

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        Text record = new Text(value);
        String recordStr = record.toString();
        if (recordStr.isEmpty()) {
            return;
        }
        String date = recordStr.substring(15, 23);
        int tempStartIdx = recordStr.charAt(87) == '+' ? 88 : 87;
        int temperature = Integer.parseInt(recordStr.substring(tempStartIdx, 92));
        int flag = Integer.parseInt(recordStr.substring(92, 93));
        if (!recordIsValid(temperature, flag)) {
            return;
        }
        context.write(new Text(date), new VIntWritable(temperature));
    }

    public boolean recordIsValid(int temperature, int flag) {
        return temperature != INVALID_TEMPERATURE_VALUE
                && qualityFlags.contains(flag);
    }
}
