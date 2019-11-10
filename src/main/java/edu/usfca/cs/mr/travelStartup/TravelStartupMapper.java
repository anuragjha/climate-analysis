package edu.usfca.cs.mr.travelStartup;

import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Line;
import edu.usfca.cs.mr.util.NCDCWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TravelStartupMapper extends Mapper<LongWritable,Text, Text, NCDCWritable> {

    String[] geoHashes = {"9q8","9q9","9qb","9qc"};

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        Set<String> geoHashSet = new HashSet<>();
        geoHashSet.addAll(Arrays.asList(geoHashes));

        String geohash = Geohash.encode(
                Float.parseFloat(Line.getLatitude(line).toString()),
                Float.parseFloat(Line.getLongitude(line).toString()),
                3
        );

        if(geoHashSet.contains(geohash)){
            System.out.println("geohash : "+ geohash);
            System.out.println("Month is : "+ Line.getUtc_date(line).toString().substring(0,6));
            context.write(new Text(Line.getUtc_date(line).toString().substring(0,6)),
                    new NCDCWritable()
                    .setAir_temperature(new DoubleWritable(Line.getAir_temperature(line)))
                    .setSoil_temperature_5(new DoubleWritable(Line.getSoil_temperature_5(line)))
                    .setRelative_humidity(new DoubleWritable(Line.getRelative_humidity(line)))
                    .setLatitude(new Text(Line.getLatitude(line)))
                    .setLongitude(new Text(Line.getLongitude(line)))
                    .setUtc_date(new Text(Line.getUtc_date(line)))
                    .setUtc_time(new Text(Line.getUtc_time(line))));
        }
    } // end of func map
}
