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

import javax.sql.rowset.CachedRowSet;
import java.util.UUID;

public class Site {
    private Logger logger = Logger.getLogger(Site.class);
    public static final ConnectorConfig connectorConfig = new ConnectorConfig();
    public static final Config config = connectorConfig.config;
    private InsertIntoCassandra cassandra = new InsertIntoCassandra();
    private InsertIntoElasticsearch elasticsearch = new InsertIntoElasticsearch();
    private HttpConnection httpConnection;
    private HSQLDBConnection hsqldbConnection;
    private String table_name;
    private String keyspace;
    private CreateDF createDF = new CreateDF();

    public Site() {
        table_name = config.getString("site.name");
        keyspace = config.getString("site.keyspace");
        httpConnection = new HttpConnection(config.getString("site.url"));
        hsqldbConnection = new HSQLDBConnection();
    }

    public void storeSitesData() {
        // need to fetch from somewhere else
        int offset;

        if (System.getProperty("offset") == null) {
            offset = hsqldbConnection.fetchLastOffset(table_name);
        } else {
            offset = Integer.parseInt(System.getProperty("offset"));
        }

        boolean enable_es = Boolean.parseBoolean(System.getProperty("enable_es", "true"));
        boolean enable_cassandra = Boolean.parseBoolean(System.getProperty("enable_cassandra", "true"));

        JSONObject payload = new JSONObject();
        payload.put("table_name", config.getString("table_mapping." + table_name));
        payload.put("offset_value", offset);

        JSONArray bg_data = httpConnection.fetch(table_name, offset, payload.toString(), hsqldbConnection);

        if (bg_data.length() > 0) {
            JSONArray final_data = new JSONArray();
            for (Object column_obj : bg_data) {
                JSONObject cols = (JSONObject) column_obj;
                cols.put("id", UUID.randomUUID().toString());
                final_data.put(cols);
            }

            Dataset<Row> df = createDF.json_to_df(final_data.toString());

            if(enable_cassandra && enable_es) {
                cassandra.insertIntoSites(df, table_name, keyspace);
                elasticsearch.insertIntoSites(df, config.getString("site.es_index"));
            }else if(enable_cassandra){
                cassandra.insertIntoSites(df, table_name, keyspace);
            }else if(enable_es){
                elasticsearch.insertIntoSites(df, config.getString("site.es_index"));
            }else{
                throw new RuntimeException("Inserting into cassandra and elasticsearch is disabled!!!");
            }
        } else {
            logger.error("Empty resultset with offset from: " + offset);
        }
    }

//    public void storeSitesData(){
//        // need to fetch from somewhere else
//        int offset;
//
////        if(System.getProperty("offset") == null){
////            offset = hsqldbConnection.fetchLastOffset(table_name);
////        }else{
////            offset = Integer.parseInt(System.getProperty("offset"));
////        }
//
//        JSONObject payload = new JSONObject();
////        payload.put("table_name", config.getString("table_mapping."+table_name));
//        payload.put("table_name", "mmpl.V_EKB_SITE");
//        payload.put("offset_value", 0);
//
//        JSONArray bg_data = httpConnection.fetch(table_name, 0, payload.toString(), hsqldbConnection);
//        int count = 1;
//        if(bg_data.length() > 0){
//            for(Object column_obj: bg_data){
//                JSONObject cols = (JSONObject) column_obj;
//
////            Iterator<String> columns = cols.keys();
////            while(columns.hasNext()){
////                String column_name = columns.next();
////                Object encrypted_text = AES.encrypt(String.valueOf(value), connectorConfig.getEncrypt_key());
////                cols.put(column_name, encrypted_text);
////            }
//
//
////                cols.put("id", UUID.randomUUID().toString());
////                final_data.put(cols);
//                System.out.println(cols);
//                hsqldbConnection.executeUpdateQuery("INSERT INTO V_SITESALE (ID, SITE_CODE, FINAL_DATE) VALUES ("+ count+ ",'"+ cols.get("site_code").toString() +"', TO_DATE('09-01-2020','dd-MM-yyyy'))");
//                count++;
//                System.out.println(count);
//            }
////            cassandra.insertIntoSites(final_data.toString(), table_name, keyspace);
////            elasticsearch.insertIntoSites(final_data.toString(), table_name);
//        }else{
////            logger.error("Empty resultset with offset from: " + offset);
//        }
//    }

}
