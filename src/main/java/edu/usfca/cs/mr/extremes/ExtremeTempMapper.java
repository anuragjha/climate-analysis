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
                .setUtc_date(Line.getUtc_date(line))
                .setUtc_time(Line.getUtc_time(line))
                .setLongitude(Line.getLongitude(line))
                .setLatitude(Line.getLatitude(line));

        if (Float.parseFloat(Line.getAir_temperature(line).toString()) >= maxTemp) {
            System.out.println("air_temp: "+ Line.getAir_temperature(line));
            System.out.println("ncdc.utc_date: "+ ncdc.getUtc_date());

            maxTemp = Float.parseFloat(Line.getAir_temperature(line).toString());

            context.write(
                    Line.getAir_temperature(line), /*Line.getUtc_date(line)*/
                    ncdc
            );
        }


    }



}