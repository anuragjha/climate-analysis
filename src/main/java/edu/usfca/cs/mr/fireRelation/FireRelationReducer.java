package edu.usfca.cs.mr.fireRelation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class FireRelationReducer
        extends Reducer<Text, FireWritable, Text, Text> {

    @Override
    protected void reduce(
            Text key, Iterable<FireWritable> values, Context context)
            throws IOException, InterruptedException {

        System.out.println("In Reducer");
        Double sumFS = 0.0;
        long count = 0;

        for(FireWritable value : values) {
           sumFS += value.getFireSize().get();
           count++;
        }
        Double avgFS = sumFS / count;

        context.write(key, new Text(String.valueOf(avgFS))); // from 2006 to 2015
        //context.write(key, new Text("ok"));


    }


}
