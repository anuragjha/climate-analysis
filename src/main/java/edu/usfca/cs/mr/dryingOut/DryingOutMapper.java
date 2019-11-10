package edu.usfca.cs.mr.dryingOut;

import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Line;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DryingOutMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    String geoHashRegion = "9q4g";

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String geohash = Geohash.encode(
                Float.parseFloat(Line.getLatitude(line)),
                Float.parseFloat(Line.getLongitude(line)),
                4
        );
//        System.out.println("geohash : "+ geohash);

        //System.out.println("Month is : "+ Line.getUtc_date(line).toString().substring(0,6));

        if (geohash.startsWith(geoHashRegion)) {
            if(Line.getPrecipitation(line) >= 0.0 && Line.getPrecipitation(line) < 9999.0) {
                context.write(
                        new Text(Line.getUtc_date(line).substring(4,6)),
                        new Text(Line.getPrecipitation(line).toString())
                );
            }
//            if(Float.parseFloat(Line.getWet_flag(line).toString()) == 0 ) {
//                context.write(
//                        new Text(Line.getUtc_date(line).toString().substring(4,6)),
//                        new Text(Line.getWetness(line).toString())
//                );
//            }

        }
    } // end of func map


}