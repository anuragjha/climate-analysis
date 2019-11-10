package edu.usfca.cs.mr.extremes;

import edu.usfca.cs.mr.util.Line;
import edu.usfca.cs.mr.util.NCDCWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ExtremeTempMapper
extends Mapper<LongWritable, Text, Text, NCDCWritable> {

    float maxTemp = (float) 0.0;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // emit air_temp, {when,where} pairs.
        String line = value.toString();

        NCDCWritable ncdc =  new NCDCWritable()
                .setUtc_date(new Text(Line.getUtc_date(line)))
                .setUtc_time(new Text(Line.getUtc_time(line)))
                .setLongitude(new Text(Line.getLongitude(line)))
                .setLatitude(new Text(Line.getLatitude(line)));

        if (Float.parseFloat(Line.getAir_temperature(line).toString()) >= maxTemp) {
            System.out.println("air_temp: "+ Line.getAir_temperature(line));
            System.out.println("ncdc.utc_date: "+ ncdc.getUtc_date());

            maxTemp = Float.parseFloat(Line.getAir_temperature(line).toString());

            context.write(
                    new Text(Line.getAir_temperature(line).toString()), /*Line.getUtc_date(line)*/
                    ncdc
            );
        }


    }



}
