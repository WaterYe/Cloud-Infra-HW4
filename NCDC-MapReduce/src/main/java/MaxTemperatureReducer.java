import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer
        extends Reducer<Text, VIntWritable, Text, VIntWritable> {

    @Override
    public void reduce(Text key, Iterable<VIntWritable> values, Context context)
            throws IOException, InterruptedException {
        int maxTemp = Integer.MIN_VALUE;
        for (VIntWritable vIntWritable : values) {
            maxTemp = Math.max(maxTemp, vIntWritable.get());
        }
        context.write(key, new VIntWritable(maxTemp));
    }

}
