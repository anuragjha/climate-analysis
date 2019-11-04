package edu.usfca.cs.mr.extreme;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ExtremeTemperatureReducer extends Reducer<Text, TemperatureDetails, Text,FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<TemperatureDetails> values,Context context) throws IOException, InterruptedException {
        float maxAirTemp = 0;
        for(TemperatureDetails detail : values ){
            if(maxAirTemp < detail.getAirTemperature()){
                maxAirTemp = detail.getAirTemperature();
            }
        }
        context.write(key,new FloatWritable(maxAirTemp));
    }
}
