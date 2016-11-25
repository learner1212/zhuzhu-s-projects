package learn.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class FullCompareReduce extends Reducer<Text, Text, Text, Text> {
    private Counter matchCounter;
    private Counter diffCounter;

    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.setup(context);
        matchCounter = context.getCounter("Count", "matchCount");
        diffCounter = context.getCounter("Count", "diffCount");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        String src_str = null;
        String des_str = null;
        for (Text text : value) {
            String line = text.toString();
            if (line.startsWith("1")) {
                src_str = line.substring(2);
            } else if (line.startsWith("2")) {
                des_str = line.substring(2);
            }
        }
        if (src_str != null && des_str != null) {
            if (src_str.equals(des_str)) {
                // matchCount++;
                // context.getCounter("Count", "matchCount").increment(1);
                matchCounter.increment(1);
            } else {
                // diffCount++;
                // context.getCounter("Count", "diffCount").increment(1);
                diffCounter.increment(1);
                String ouput_value = "0;" + key + ";" + src_str + ";" + des_str;
                context.write(null, new Text(ouput_value));
            }

        } else if (src_str != null) {
            // diffCount++;
            // context.getCounter("Count", "diffCount").increment(1);
            diffCounter.increment(1);
            String ouput_value = "1;" + key + ";" + src_str;
            context.write(null, new Text(ouput_value));

        } else if (des_str != null) {
            // diffCount++;
            // context.getCounter("Count", "diffCount").increment(1);
            diffCounter.increment(1);
            String ouput_value = "2;" + key + ";" + des_str;
            context.write(null, new Text(ouput_value));
        } else {

        }
    }

}
