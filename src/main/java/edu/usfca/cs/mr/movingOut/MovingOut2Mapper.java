package edu.usfca.cs.mr.movingOut;

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

public class MovingOut2Mapper
        extends Mapper<LongWritable, Text, Text, NCDCWritable> {

    String[] geoHashes = {"9qb", "9qc", "9q9", "9qd", "9qe", "9q3", "9q6", "9q7", "9qk", "9q4", "9q5", "9qh", "9qj", "9mu", "9mv"};

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        Set<String> geoHashesSet = new HashSet<>();
        geoHashesSet.addAll(Arrays.asList(geoHashes));

        String geohash = Geohash.encode(
                Float.parseFloat(Line.getLatitude(line)),
                Float.parseFloat(Line.getLongitude(line)),
                3
        );


        if (!geoHashesSet.contains(geohash)) { // todo : change to not contains
            System.out.println("geohash : "+ geohash);
            if(Float.parseFloat(Line.getPrecipitation(line).toString()) >= 0.0 ) {
                context.write(
                        new Text(geohash),
                        new NCDCWritable()
                                .setGeohash(new Text(geohash))
                                .setAir_temperature(new DoubleWritable(Line.getAir_temperature(line)))
                                .setPrecipitation(new DoubleWritable(Line.getPrecipitation(line)))
                                .setSolar_radiation(new DoubleWritable(Line.getSolar_radiation(line)))
                );
            }
        }
    } // end of func map


}