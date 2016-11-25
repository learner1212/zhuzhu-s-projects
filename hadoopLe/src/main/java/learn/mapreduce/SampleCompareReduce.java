package learn.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class SampleCompareReduce extends Reducer<Text, Text, Text, Text> {

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
        String ouput_value = "";
        if (src_str != null && des_str != null) {
            if (src_str.equals(des_str)) {
                // matchCount++;
                // context.getCounter("Count", "matchCount").increment(1);
                matchCounter.increment(1);
                ouput_value = "0\003 matched\002 record:" + src_str + "\003" + Format.getDateTime();
                // context.write(new Text(key), new Text(ouput_value));
                // database.addBatch(new Object[] { 0, "匹配",
                // Format.getDateTime() });
            } else {
                // diffCount++;
                // context.getCounter("Count", "diffCount").increment(1);
                diffCounter.increment(1);
                ouput_value = "1\003 not matched\002 mysql:" + src_str + "\002 hive:" + des_str + "\003" + Format.getDateTime();
                // database.addBatch(new Object[] { 1, ouput_value,
                // Format.getDateTime() });
                // context.write(new Text(key), new Text(ouput_value));
            }

        } else if (src_str != null) {
            // diffCount++;
            // context.getCounter("Count", "diffCount").increment(1);
            diffCounter.increment(1);
            ouput_value = "2\003 not found in hive\002 mysql:" + src_str + "\003" + Format.getDateTime();
            // database.addBatch(new Object[] { 2, ouput_value,
            // Format.getDateTime() });
            // context.write(new Text(key), new Text(ouput_value));

        } else if (des_str != null) {
            // diffCount++;
            // context.getCounter("Count", "diffCount").increment(1);
            diffCounter.increment(1);
            ouput_value = "3\003 not found in mysql\002 hive:" + des_str + "\003" + Format.getDateTime();
            // database.addBatch(new Object[] { 3, ouput_value,
            // Format.getDateTime() });
            // context.write(new Text(key), new Text(ouput_value));
        } else {

        }
        context.write(null, new Text(key + "\003" + ouput_value));
    }

    // @Override
    // protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
    // throws IOException, InterruptedException {
    // // TODO Auto-generated method stub
    // if (database != null) {
    // database.commit();
    // database.close();
    // }
    // super.cleanup(context);
    // }
}
