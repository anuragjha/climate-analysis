package edu.usfca.cs.mr.extremes;

import edu.usfca.cs.mr.util.NCDCWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ExtremeTempReducer
        extends Reducer<Text, NCDCWritable, Text, Text> {

    float maxTemp = (float) 0.0;

    @Override
    protected void reduce(
            Text key, Iterable<NCDCWritable> values, Context context)
            throws IOException, InterruptedException {


        int count = 0;
        NCDCWritable last = new NCDCWritable();

        if (Float.parseFloat(key.toString()) >= maxTemp) {
            System.out.println("Float.parseFloat(key.toString()) > maxTemp");
            maxTemp = Float.parseFloat(key.toString());

            for (NCDCWritable val : values) {
                System.out.println("val : " + val.getUtc_date().toString());
                //utc_date = val.getUtc_date();
                last = val;
                count += 1;
            }

            context.write(key, new Text(last.toString()));
        }

    }



}
