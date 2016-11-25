package learn.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import learn.mapreduce.path.verifyDataConf;

public class destMap extends Mapper<LongWritable, Text, Text, Text> {
    private String indexes = "";

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.setup(context);
        Configuration configuration = context.getConfiguration();
        indexes = configuration.get(verifyDataConf.KEYBITSET);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        context.getCounter("Count", "destCount").increment(1);
        String line = value.toString();
        String[] value_arr = line.split("\001");
        List<String> new_key_list = new ArrayList<String>();
        for (int i = 0; i < indexes.length(); i++) {
            if (indexes.charAt(i) == '1') {
                new_key_list.add(value_arr[i]);
            }
        }
        String newKey = StringUtils.join(new_key_list, '\t');
        context.write(new Text(newKey), new Text("2;" + line));
    }

}
