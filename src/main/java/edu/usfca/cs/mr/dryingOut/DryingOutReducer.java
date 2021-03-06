package edu.usfca.cs.mr.dryingOut;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class DryingOutReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        //System.out.println("In Reducer");
        float sumP = 0;
        int count = 0;
        for (DoubleWritable val : values) {
            count++;
            sumP += val.get();//Double.parseDouble(val.toString());
        }

        double avgP = sumP / count;
        //System.out.println("AvgP : "+ avgP);
        context.write(key, new Text(String.valueOf(avgP)));
    }
}
