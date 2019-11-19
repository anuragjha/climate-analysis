package edu.usfca.cs.mr.FireCorrelation;

import edu.usfca.cs.mr.pcc.RunningStatisticsND;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FireMapper extends Mapper<LongWritable, Text, Text, RunningStatisticsND> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] array = line.split("\\s+");

        System.out.println("In Mapper !!");
        String geohash = array[0];
        Double fireSize =Double.parseDouble(array[1]);
        Double airTemp = Double.parseDouble(array[2]);
        Double precipitation = Double.parseDouble(array[3]);
        Double solar = Double.parseDouble(array[4]);
        Double wind = Double.parseDouble(array[5]);

        System.out.println("fireSize : "+fireSize);
        System.out.println("Wind : "+wind);
        RunningStatisticsND nd = new RunningStatisticsND(5);
        nd.put(new double[]{fireSize,airTemp,precipitation,solar, wind});

        context.write(new Text(String.valueOf(1)), nd);
    }

}
