package edu.usfca.cs.mr.travelStartup;

import edu.usfca.cs.mr.util.NCDCWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class TravelStartupReducer extends Reducer<Text, NCDCWritable,Text,Text> {

    float bestComfortIndex = 0;
    String geoHashForBestComfortIndex = "";

    @Override
    protected void reduce(Text key, Iterable<NCDCWritable> values, Context context)
            throws IOException, InterruptedException{

        HashMap<String,TotalTempAndHumidity> geohashToTotal = new HashMap<>();


        for(NCDCWritable ncdcWritable : values){
            String geohash = ncdcWritable.getGeohash().toString();
            float airTemp = Float.parseFloat(ncdcWritable.getAir_temperature().toString());
            float relativeHumidity = Float.parseFloat(ncdcWritable.getRelative_humidity().toString());

            if(geohashToTotal.containsKey(geohash)){
                TotalTempAndHumidity tth = geohashToTotal.get(geohash);
                tth.setAirtemp(tth.getAirtemp() + airTemp);
                tth.setHumidity(tth.getHumidity() + relativeHumidity);
                tth.setCount(1);
                geohashToTotal.put(geohash,tth);
            }else {
                TotalTempAndHumidity tth = new TotalTempAndHumidity(airTemp,relativeHumidity,1);
                geohashToTotal.put(geohash, tth);
            }
        }
        for (String geo : geohashToTotal.keySet()){
            TotalTempAndHumidity tth = geohashToTotal.get(geo);
            float avgAirTemp =tth.getAirtemp()/tth.getCount();
            float avgHumidity = tth.getHumidity()/tth.getCount();
            tth.setComfortIndex((avgAirTemp + avgHumidity)/40);
            if(tth.getComfortIndex() > 2.30 && tth.getComfortIndex() < 9){
                bestComfortIndex = tth.getComfortIndex();
                geoHashForBestComfortIndex = geo;
                System.out.println("bestComfortIndex : "+bestComfortIndex+" "+geoHashForBestComfortIndex+" "+key);
            }
        }
        String geoMonth = geoHashForBestComfortIndex + " "+key;
        context.write(new Text(geoMonth),new Text(String.valueOf(bestComfortIndex)));
    }
}
