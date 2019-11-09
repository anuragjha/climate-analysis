package edu.usfca.cs.mr.travelStartup;

import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Line;
import edu.usfca.cs.mr.util.NCDCWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TravelStartupMapper extends Mapper<LongWritable,Text, Text, NCDCWritable> {

    String[] geoHashes = {"9q4","9q9","9qb","9qe"};

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
            System.out.println("Month is : "+ Line.getUtc_date(line).toString().substring(4,6));
            context.write(new Text(Line.getUtc_date(line).toString().substring(4,6)),
                    new NCDCWritable()
                    .setAir_temperature(Line.getAir_temperature(line))
                    .setSoil_temperature_5(Line.getSoil_temperature_5(line))
                    .setRelative_humidity(Line.getRelative_humidity(line))
                    .setLatitude(Line.getLatitude(line))
                    .setLongitude(Line.getLongitude(line))
                    .setUtc_date(Line.getUtc_date(line))
                    .setUtc_time(Line.getUtc_time(line))
                    .setGeohash(new Text(geohash)));
        }
    } // end of func map
}
