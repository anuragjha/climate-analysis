package edu.usfca.cs.mr.fireRelation;

import edu.usfca.cs.mr.extremes.ETWritable;
import edu.usfca.cs.mr.util.Geohash;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FireRelationMapper
        extends Mapper<LongWritable, Text, Text, FireWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        try {
            String[] fields = line.split(",");

            if (fields.length >= 9) {
                Float latitude = Float.parseFloat(fields[7]);
                Float longitude = Float.parseFloat(fields[8]);
                String geohash = Geohash.encode(latitude, longitude, 3);
                String year = fields[2];
                Double fireSize = Double.parseDouble(fields[5]);

                FireWritable fw = new FireWritable()
                        .setLatitude(new FloatWritable(latitude))
                        .setLongitude(new FloatWritable(longitude))
                        .setGeohash(new Text(geohash))
                        .setYear(new Text(year))
                        .setFireSize(new DoubleWritable(fireSize));//latitude, longitude, geohash, year, fireSize);


                //System.out.println(fw.toString());

                context.write(new Text(geohash), fw);
            }

        } catch (Exception e) {
            System.out.println("Exception");
        }



    }



}
