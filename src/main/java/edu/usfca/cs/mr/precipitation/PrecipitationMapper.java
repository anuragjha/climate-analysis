package edu.usfca.cs.mr.precipitation;

import edu.usfca.cs.mr.util.Geohash;
import edu.usfca.cs.mr.util.Line;
import edu.usfca.cs.mr.util.NCDCWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PrecipitationMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    String geoHashRegion = "9";

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String geohash = Geohash.encode(
                Float.parseFloat(Line.getLatitude(line).toString()),
                Float.parseFloat(Line.getLongitude(line).toString()),
                1
        );
        System.out.println("geohash : "+ geohash);

        System.out.println("Month is : "+ Line.getUtc_date(line).toString().substring(0,6));

        if (geohash.startsWith(geoHashRegion)) {
            if(Float.parseFloat(Line.getPrecipitation(line).toString()) >= 0.0 ) {
                context.write(
                        new Text(Line.getUtc_date(line).toString().substring(0,6)),
                        new Text(Line.getPrecipitation(line))
                );
            }
        }
    } // end of func map


}