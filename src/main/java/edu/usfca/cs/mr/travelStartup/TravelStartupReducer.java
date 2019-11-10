package edu.usfca.cs.mr.travelStartup;

import edu.usfca.cs.mr.util.NCDCWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class TravelStartupReducer extends Reducer<Text, NCDCWritable,Text,Text> {

    private float bestComfortIndex = 0;
    private String geoHashForBestComfortIndex = "";

    @Override
    protected void reduce(Text key, Iterable<NCDCWritable> values, Context context)
            throws IOException, InterruptedException{

        HashMap<String,TotalTempAndHumidity> geohashToTotal = new HashMap<>();
        System.out.println("In Reducer!");

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
            System.out.print("String goe : "+geo);
            TotalTempAndHumidity tth = geohashToTotal.get(geo);
            float avgAirTemp =tth.getAirtemp()/tth.getCount();
            float avgHumidity = tth.getHumidity()/tth.getCount();
            tth.setComfortIndex((avgAirTemp + avgHumidity)/40);
            System.out.println("  Comfort Index : "+tth.getComfortIndex());
            if(tth.getComfortIndex() > 0 && tth.getComfortIndex() < 9){
                if(tth.getComfortIndex() > bestComfortIndex) {
                    bestComfortIndex = tth.getComfortIndex();
                    geoHashForBestComfortIndex = geo;
                }
                System.out.println("bestComfortIndex : "+bestComfortIndex+" "+geoHashForBestComfortIndex+" "+key);
            }
        }
        String geoMonth = geoHashForBestComfortIndex + " "+key;
        context.write(new Text(geoMonth),new Text(String.valueOf(bestComfortIndex)));
    }
}
