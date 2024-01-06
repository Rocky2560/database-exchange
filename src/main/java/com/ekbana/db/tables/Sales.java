package com.ekbana.db.tables;

import com.ekbana.db.concrete_classes.HSQLDBConnection;
import com.ekbana.db.config.ConnectorConfig;
import com.ekbana.db.connections.HttpConnection;
import com.ekbana.db.dbencrypt.Encrypt;
import com.ekbana.db.util.GetDate;
import com.typesafe.config.Config;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import spark.CreateDF;
import spark.InsertIntoCassandra;
import spark.InsertIntoElasticsearch;
import spark.SaveToCSV;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class Sales {
    private Logger logger = Logger.getLogger(Sales.class);
    public static final ConnectorConfig connectorConfig = new ConnectorConfig();
    public static final Config config = connectorConfig.config;
    private InsertIntoCassandra cassandra = new InsertIntoCassandra();
    private InsertIntoElasticsearch elasticsearch = new InsertIntoElasticsearch();
    private SaveToCSV saveToCSV = new SaveToCSV();
    private Encrypt encrypt = new Encrypt();
    private final DateFormat dateFormat = new SimpleDateFormat("dd-MMM-yy");
    private GetDate getDate = new GetDate();
    private HttpConnection httpConnection;
    private HSQLDBConnection hsqldbConnection;
    private String keyspace;
    private String date_column;
    private CreateDF createDF = new CreateDF();

    public Sales(){
        keyspace = config.getString("sales.keyspace");
        date_column = config.getString("sales.date_column");
        hsqldbConnection = new HSQLDBConnection();
    }

    public void storeSalesData(int request){
        String start_date = System.getProperty("start_date");
        String final_date = System.getProperty("final_date");

        boolean enable_es = Boolean.parseBoolean(System.getProperty("enable_es", "true"));
        boolean enable_cassandra = Boolean.parseBoolean(System.getProperty("enable_cassandra", "true"));

        if(start_date == null && final_date == null){
            // getting one day before date
//            start_date = getDate.addDate(new Date(), -1);
            if(request == 3){
                start_date = hsqldbConnection.fetchLastFetchedDate(config.getString("sales.enc_bill_table"));
            }else{
                start_date = hsqldbConnection.fetchLastFetchedDate(config.getString("sales.enc_table"));
            }
            final_date = getDate.addDate(start_date, 1);
        }else if (start_date == null || final_date == null){
            throw new RuntimeException("both start_date and final_date should be provided");
        }

        try {
            Date s_date = dateFormat.parse(start_date);
            Date f_date = dateFormat.parse(final_date);

            String csv_dir = "";

            while (s_date.compareTo(f_date) < 0){
                // converting start_date to dd-MMM-yy format
                String d1 = getDate.getFormatedDate(s_date);
                // increasing start_date with 1 day and changing its format
                String d2 = getDate.addDate(s_date, 1);
                Date end_date = dateFormat.parse(d2);

                logger.info("startDate: " + d1 + ", endDate: " + d2);
                s_date = end_date;

                // fetch data
                JSONArray final_data;
                httpConnection = new HttpConnection(config.getString("sales.enc_url"));

                if(request == 2){
                    String table_name = config.getString("sales.enc_table");
                    final_data = encrypt_columns(d1, d2, table_name, true);
                    insert_sales_record(enable_es, enable_cassandra, d1, d2, final_data, table_name);
                }
                else if(request == 3){
                    String table_name = config.getString("sales.enc_bill_table");
                    final_data = encrypt_columns(d1, d2, table_name, false);
                    insert_collection_records(enable_es, enable_cassandra, d1, d2, final_data, table_name);
                }
                else{
                    throw new RuntimeException("Invalid request to fetch sales data!!!");
                }

            }
        } catch (ParseException e) {
            logger.error(e);
        }
    }

    private void insert_sales_record(boolean enable_es, boolean enable_cassandra, String d1, String d2,
                                JSONArray final_data, String table_name) {
        if (final_data.length() > 0){

            Dataset<Row> df = createDF.json_to_df(final_data.toString());
            String csv_path = config.getString("csv_path").concat("/").concat("sales");

            if(enable_cassandra && enable_es){
                cassandra.insertIntoSales(df, table_name, keyspace);
                elasticsearch.insertIntoSales(df, table_name);
                saveToCSV.save(df, csv_path, d1);
            }
            else if(enable_cassandra){
                cassandra.insertIntoSales(df, table_name, keyspace);
                saveToCSV.save(df, csv_path, d1);
            }
            else if(enable_es){
                elasticsearch.insertIntoSales(df, table_name);
                saveToCSV.save(df, csv_path, d1);
            }else{
                saveToCSV.save(df, csv_path, d1);
                throw new RuntimeException("Inserting into cassandra and elasticsearch is disabled!!!");
            }
        }else {
            logger.error("Empty resultset for start_date: " + d1 + " and end_date: " + d2);
        }
    }

    private void insert_collection_records(boolean enable_es, boolean enable_cassandra, String d1, String d2,
                                JSONArray final_data, String table_name) {
        if (final_data.length() > 0){

            Dataset<Row> df = createDF.json_to_df(final_data.toString());
            String csv_path = config.getString("csv_path").concat("/").concat("collection");

            if(enable_cassandra && enable_es){
                cassandra.insertIntoCollection(df, table_name, keyspace);
                elasticsearch.insertIntoCollection(df, table_name);
                saveToCSV.save(df, csv_path, d1);
            }
            else if(enable_cassandra){
                cassandra.insertIntoCollection(df, table_name, keyspace);
                saveToCSV.save(df, csv_path, d1);
            }
            else if(enable_es){
                elasticsearch.insertIntoCollection(df, table_name);
                saveToCSV.save(df, csv_path, d1);
            }else{
                saveToCSV.save(df, csv_path, d1);
                throw new RuntimeException("Inserting into cassandra and elasticsearch is disabled!!!");
            }
        }else {
            logger.error("Empty resultset for start_date: " + d1 + " and end_date: " + d2);
        }
    }

    private void to_csv(Dataset<Row> df, String date){
        try {
            String storage = config.getString("sales_csv");
            df.coalesce(1).write().option("header", "true").csv("file://" + storage + "/" + date);
        }catch (Exception e){
            logger.error(e.getMessage());
        }
    }

    private JSONArray encrypt_columns(String start_date, String end_date, String table_name, boolean loyaltycheck){
        JSONObject payload = new JSONObject();
        payload.put("start_date", start_date);
        payload.put("end_date", end_date);
        payload.put("date_column", date_column);
        payload.put("table_name", config.getString("table_mapping."+table_name));

        JSONArray bg_data = httpConnection.fetch(table_name, keyspace, start_date, end_date, payload.toString(),
                hsqldbConnection);

        logger.info("data size: " + bg_data.length());
        if(bg_data == null){
            logger.info("data is null");
        }

        JSONArray final_data = new JSONArray();
        for(Object column_obj: bg_data){
            JSONObject cols = (JSONObject) column_obj;
            if(loyaltycheck){
                if(!cols.has("loyaltydiscount")){
                    cols.put("loyaltydiscount", 0);
                }
            }
            cols.put("id", UUID.randomUUID().toString());
            final_data.put(cols);
        }

        return final_data;
    }
}
