package edu.usfca.cs.mr.movingOut;

import edu.usfca.cs.mr.util.Config;
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

public class MovingOut1Mapper
        extends Mapper<LongWritable, Text, Text, NCDCWritable> {

    String bayAreaGeoHash = Config.readConfig("config.json").getMovingOutGeoHash();//"9qb";
    //String[] geoHashes = {"9qb"/*, "9qc", "9qf", "9qg", "9q9", "9qd", "9qe", "9qs", "9q3", "9q6", "9q7", "9qk", "9q4", "9q5", "9qh", "9qj", "9mu", "9mv"*/};

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();


        String geohash = Geohash.encode(
                Float.parseFloat(Line.getLatitude(line)),
                Float.parseFloat(Line.getLongitude(line)),
                bayAreaGeoHash.length()
        );

//        if (bayAreaGeoHash.equalsIgnoreCase(geohash)) {
            //System.out.println("geohash : "+ geohash);
            if(isCleanData(line) ) {
                context.write(
                        new Text(geohash),
                        new NCDCWritable()
                                .setGeohash(new Text(geohash))
                                .setLatitude(new Text(Line.getLatitude(line)))
                                .setLongitude(new Text(Line.getLongitude(line)))
                                .setAir_temperature(new DoubleWritable(Line.getAir_temperature(line)))
                                .setPrecipitation(new DoubleWritable(Line.getPrecipitation(line)))
                                .setWind_1_5(new DoubleWritable(Line.getWind_1_5(line)))
                );
            }
//        }
    } // end of func map


    private boolean isCleanData(String line) {
        if (Line.getAir_temperature(line) >= 500) {
            return false;
        } else if (Line.getAir_temperature(line) <= -500) {
            return false;
        }

        if(Line.getPrecipitation(line) < 0.0 || Line.getPrecipitation(line) > 2000) {
            return false;
        }

        if(!(Line.getWind_flag(line)).equalsIgnoreCase("0")) {
            return false;
        }
        if(Line.getWind_1_5(line) < 0.0) {
            return false;
        }

        return true;
    }


}