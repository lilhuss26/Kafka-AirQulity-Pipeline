package org.database.Influx;

import com.influxdb.v3.client.InfluxDBClient;
import org.models.LocationRecord;
import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.write.WriteOptions;

public class InfluxInsert {
    private InfluxConfig config = new InfluxConfig();
    private InfluxDBClient client = config.getClient();
    private String db = "Air-Qulity";

    public void insertRecord(LocationRecord record){
        try {
            Point pointer = Point.measurement("air-quality")
                    .setTag("location_id", String.valueOf(record.getLocationsId()))
                    .setTag("sensor_id", String.valueOf(record.getSensorsId()))
                    .setTag("lat", String.valueOf(record.getLatitude()))
                    .setTag("lon", String.valueOf(record.getLongitude()))
                    .setField("value", record.getValue())
                    .setTimestamp(record.getDatetime());

            client.writePoint(pointer, new WriteOptions.Builder().database(db).build());
            System.out.println("Inserted to Influxdb");
        }catch (Exception e){
            System.err.println(e.getMessage());
        }
    }
}