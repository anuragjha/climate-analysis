package edu.usfca.cs.mr.dryingOut;

import edu.usfca.cs.mr.util.Config;
import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Line;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DecimalFormat;

public class DryingOutMapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    Config config = Config.readConfig("config.json");

    String geoHashRegion = config.getDryingOutGeoHash();//"9q4g";

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String geohash = Geohash.encode(
                Float.parseFloat(Line.getLatitude(line)),
                Float.parseFloat(Line.getLongitude(line)),
                geoHashRegion.length()
        );
//        System.out.println("geohash : "+ geohash);

        //System.out.println("Month is : "+ Line.getUtc_date(line).toString().substring(0,6));

        if (geohash.equalsIgnoreCase(geoHashRegion)) {
            if(Line.getPrecipitation(line) >= 0.0 && Line.getPrecipitation(line) < 2000) {

//                System.out.println();
                context.write(
                        new Text(Line.getUtc_date(line).substring(4,6)),
                        //new Text(String.valueOf(Line.getPrecipitation(line)))
                        new DoubleWritable(Line.getPrecipitation(line))
                );
            }
//            if(Float.parseFloat(Line.getWet_flag(line)) == 0 ) {
//                context.write(
//                        new Text(Line.getUtc_date(line).substring(4,6)),
//                        new DoubleWritable(Line.getWetness(line))
//                );
//            }

        }
    } // end of func map


}