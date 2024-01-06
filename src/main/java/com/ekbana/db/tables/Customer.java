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
import java.util.UUID;

public class Customer {
    private Logger logger = Logger.getLogger(Customer.class);
    public static final ConnectorConfig connectorConfig = new ConnectorConfig();
    public static final Config config = connectorConfig.config;
    private InsertIntoCassandra cassandra = new InsertIntoCassandra();
    private InsertIntoElasticsearch elasticsearch = new InsertIntoElasticsearch();
    private HttpConnection httpConnection;
    private HSQLDBConnection hsqldbConnection;
    private String table_name;
    private String keyspace;
    private CreateDF createDF = new CreateDF();

    public Customer(){
        table_name = config.getString("cust.name");
        keyspace = config.getString("cust.keyspace");
        httpConnection = new HttpConnection(config.getString("cust.url"));
        hsqldbConnection = new HSQLDBConnection();
    }
    public void storeCustomerData(){
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
                JSONObject filtered_cols = new JSONObject();
                Iterator<String> columns = cols.keys();
                while(columns.hasNext()){
                    String column_name = columns.next();
//                Object encrypted_text = AES.encrypt(String.valueOf(value), connectorConfig.getEncrypt_key());
//                cols.put(column_name, encrypted_text);
                    if(column_name.equals("lpcardno") || column_name.equals("code")){
                        filtered_cols.put(column_name, cols.get(column_name));
                    }
                }
                filtered_cols.put("id", UUID.randomUUID().toString());
                final_data.put(filtered_cols);
            }

            Dataset<Row> df = createDF.json_to_df(final_data.toString());

            cassandra.insertIntoCustomer(df, table_name, keyspace);
            elasticsearch.insertIntoCustomers(df, table_name);
        }else{
            logger.error("Empty resultset with offset from: " + offset);
        }
    }
}
