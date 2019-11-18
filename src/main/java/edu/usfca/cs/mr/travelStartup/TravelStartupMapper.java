package edu.usfca.cs.mr.travelStartup;

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

public class TravelStartupMapper extends Mapper<LongWritable,Text, Text, NCDCWritable> {

    Config config = Config.readConfig("config.json");
    String[] geoHashes = config.getTravelStartupGeoHashes();//{"9q4","9qd","9v6","dh4","8e3"};

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        Set<String> geoHashSet = new HashSet<>();
        geoHashSet.addAll(Arrays.asList(geoHashes));

        String geohash = Geohash.encode(
                Float.parseFloat(Line.getLatitude(line)),
                Float.parseFloat(Line.getLongitude(line)),
                geoHashes[0].length()
        );

        double airTemp = Line.getAir_temperature(line);
        airTemp = (airTemp * 9/5) + 32;

        if(geoHashSet.contains(geohash) && isCleanData(line)){
            context.write(new Text(Line.getUtc_date(line).substring(4,6)),
                    new NCDCWritable()
                    .setAir_temperature(new DoubleWritable(airTemp))
                    .setRelative_humidity(new DoubleWritable(Line.getRelative_humidity(line)))
                    .setLatitude(new Text(Line.getLatitude(line)))
                    .setLongitude(new Text(Line.getLongitude(line)))
                    .setUtc_date(new Text(Line.getUtc_date(line)))
                    .setUtc_time(new Text(Line.getUtc_time(line)))
                    .setGeohash(new Text(geohash)));
        }
    } // end of func map

    private boolean isCleanData(String line) {
        if(Line.getAir_temperature(line) >= 500) {
            return false;
        } else if (Line.getAir_temperature(line) <= -500) {
            return false;
        }

        if(Integer.parseInt(Line.getRh_flag(line)) != 0) {
            return false;
        }

        return true;
    }
}
