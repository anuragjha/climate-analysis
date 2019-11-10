package edu.usfca.cs.mr.dryingOut;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DryingOutReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        //System.out.println("In Reducer");
        float sumP = 0;
        int count = 0;
        for (Text val : values) {
            count++;
            sumP += Double.parseDouble(val.toString());
        }

        double avgP = sumP / count;
        //System.out.println("AvgP : "+ avgP);
        context.write(key, new Text(String.valueOf(avgP)));
    }


}
