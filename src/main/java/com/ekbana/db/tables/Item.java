package com.ekbana.db.tables;

import com.ekbana.db.concrete_classes.HSQLDBConnection;
import com.ekbana.db.config.ConnectorConfig;
import com.ekbana.db.connections.HttpConnection;
import com.typesafe.config.Config;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import spark.CreateDF;
import spark.InsertIntoCassandra;
import spark.InsertIntoElasticsearch;

import java.util.Iterator;

public class Item {
    private Logger logger = Logger.getLogger(Item.class);
    public static final ConnectorConfig connectorConfig = new ConnectorConfig();
    public static final Config config = connectorConfig.config;
    private InsertIntoCassandra cassandra = new InsertIntoCassandra();
    private InsertIntoElasticsearch elasticsearch = new InsertIntoElasticsearch();
    private HttpConnection httpConnection;
    private HSQLDBConnection hsqldbConnection;
    private String table_name;
    private String keyspace;
    private CreateDF createDF = new CreateDF();

    public Item(){
        table_name = config.getString("item.name");
        keyspace = config.getString("item.keyspace");
        httpConnection = new HttpConnection(config.getString("item.url"));
        hsqldbConnection = new HSQLDBConnection();
    }

    public void storeItemsData(){
        // need to fetch from somewhere else
        int offset;

        if(System.getProperty("offset") == null){
            offset = hsqldbConnection.fetchLastOffset(table_name);
        }else{
            offset = Integer.parseInt(System.getProperty("offset"));
        }

        JSONObject payload = new JSONObject();
        payload.put("table_name", config.getString("table_mapping."+table_name));
        payload.put("offset_value", offset);

        JSONArray bg_data = httpConnection.fetch(table_name, offset, payload.toString(), hsqldbConnection);

        if(bg_data.length() > 0){
            JSONArray final_data = new JSONArray();
            for(Object column_obj: bg_data){
                JSONObject cols = (JSONObject) column_obj;
                JSONObject cols_data = new JSONObject();

                Iterator<String> columns = cols.keys();
                while(columns.hasNext()){
                    try{
                        String column_name = columns.next();
                        String mapped_column_name = config.getString("item.columns_mapping."+column_name);
                        Object value = cols.get(column_name);
                        cols_data.put(mapped_column_name, value);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
                final_data.put(cols_data);
            }

            Dataset<Row> df = createDF.json_to_df(final_data.toString());

            cassandra.insertIntoItems(df, table_name, keyspace);
            elasticsearch.insertIntoItems(df, table_name);
        }else{
            logger.error("Empty resultset with offset from: " + offset);
        }
    }
}
