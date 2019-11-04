package edu.usfca.cs.mr.extreme;

import edu.usfca.cs.mr.util.Geohash;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class ExtremeTemperatureMapper extends Mapper<LongWritable,Text, Text, TemperatureDetails> {

    public static final int MISSING = 9999;


    @Override
    public void map(LongWritable longWritable, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        Long date = Long.parseLong(line.substring(7,14));
        Long time = Long.parseLong(line.substring(16,19));
        Float latitude = Float.parseFloat(line.substring(50,56));
        Float longitude = Float.parseFloat(line.substring(42,48));
        Float airTemp = Float.parseFloat(line.substring(58,64));
        Float surfaceTemp = Float.parseFloat(line.substring(83,89));

        System.out.println("Line :" +airTemp);

        String key = Geohash.encode(Float.valueOf(latitude),Float.valueOf(longitude),10);

        TemperatureDetails details = new TemperatureDetails();

        details.set(new FloatWritable(airTemp),new FloatWritable(surfaceTemp),new FloatWritable(latitude),new FloatWritable(longitude),new LongWritable(date),new LongWritable(time));

        System.out.println(details.toString());

        if(new Float(airTemp) != MISSING) {
            //context.write(new Text(key), new FloatWritable(new Float(airTemp)));
            context.write(new Text(key),details);
        }
    }
}
