package com.ekbana.db.tables;

import com.ekbana.db.concrete_classes.HSQLDBConnection;
import com.ekbana.db.config.ConnectorConfig;
import com.ekbana.db.connections.HttpConnection;
import com.ekbana.db.dbencrypt.Encrypt;
import com.ekbana.db.util.GetDate;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json4s.jackson.Json;
import org.mortbay.util.ajax.JSON;
import spark.CreateDF;
import spark.InsertIntoCassandra;
import spark.InsertIntoElasticsearch;

import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SalesSite {
    private Logger logger = Logger.getLogger(Sales.class);
    private Logger lg = Logger.getRootLogger();
    public static final ConnectorConfig connectorConfig = new ConnectorConfig();
    public static final Config config = connectorConfig.config;
    private InsertIntoCassandra cassandra = new InsertIntoCassandra();
    private InsertIntoElasticsearch elasticsearch = new InsertIntoElasticsearch();
    private Encrypt encrypt = new Encrypt();
    private final DateFormat dateFormat = new SimpleDateFormat("dd-MMM-yy");
    private final DateFormat dateFormat_tch = new SimpleDateFormat("dd-MMM-yy");
    private GetDate getDate = new GetDate();
    private HttpConnection httpConnection;
    private HSQLDBConnection hsqldbConnection;
    private String keyspace;
    private String table_name;
    private String date_column;
    private Date today_date = new Date();
    private boolean site_there = true;
    private CreateDF createDF = new CreateDF();


    public SalesSite() {
        keyspace = config.getString("sales.keyspace");
        hsqldbConnection = new HSQLDBConnection();
    }

    public void storeSalesData(int request) throws ParseException {
        List<String> myList;
        String start_date = System.getProperty("start_date");
        String final_date = System.getProperty("final_date");
        String store_site_data = "";
        if (start_date == null && final_date == null) {
            JSONArray site_data = new JSONArray();
            site_data = hsqldbConnection.fetchSiteCode1();
            for (Object column_obj : site_data) {
                JSONObject cols = (JSONObject) column_obj;
//                    System.out.println(cols);
                if (cols.has("final_date")) {
                    start_date = (String) cols.get("final_date");
                    try {
                        // *** same for the format String below
                        start_date = dateFormat.format(new SimpleDateFormat("yyyy-MM-dd").parse(start_date));

                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
                if (cols.has("site_code")) {
                    store_site_data = (String) cols.get("site_code");
                }
                final_date = dateFormat.format(today_date);
                myList = new ArrayList<>(Arrays.asList(store_site_data.split(",")));
//                String temp = StringUtils.join(myList, "\", \"");
//                String temp2 = StringUtils.wrap(temp, "\"");
//                List<String> list_final = new ArrayList<String>(Arrays.asList(temp2));

                try {
                    System.out.println("FETCH FOR SITE_CODE = " + myList);
                    Date s_date = dateFormat.parse(start_date);
                    Date f_date = dateFormat.parse(final_date);
                    while (s_date.compareTo(f_date) < 0) {
                        // converting start_date to dd-MMM-yy format
                        String d1 = getDate.getFormatedDate(s_date);
                        // increasing start_date with 1 day and changing its format
                        String d2 = getDate.addDate(s_date, 1);
                        Date end_date = dateFormat.parse(d2);

                        logger.info("start_date: " + d1 + ", end_date: " + d2);
                        s_date = end_date;

                        // fetch data
                        JSONArray final_data;
                        if (request == 7) {
                            table_name = config.getString("sales.enc_table");
                            httpConnection = new HttpConnection(config.getString("sales.fetch_sale"));
                            final_data = fetchSale(d1, d2, myList);
//                            System.out.println(final_data);
                        } else {
                            throw new RuntimeException("Invalid request to fetch sales data!!!");
                        }

                        // insert into cassandra
                        if (final_data.length() > 0) {
                            if (site_there) {
                                Dataset<Row> df = createDF.json_to_df(final_data.toString());

                                cassandra.insertIntoSales(df, table_name, keyspace);
//                        elasticsearch.insertIntoSales(final_data.toString(), table_name);
                            } else {
                                logger.error("Empty resultset for start_date: " + d1 + " and end_date: " + d2);
                            }
                        }
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                    logger.error(e.getMessage());
                    lg.error(e.getMessage());
                }
            }
        }
    }

    private JSONArray fetchSale(String start_date, String end_date, List<String> store_site){
        JSONArray site_check;
        JSONObject payload = new JSONObject();
        payload.put("start_date", start_date);
        payload.put("end_date", end_date);
        payload.put("site_code", store_site);
        JSONArray result_array;
        JSONArray bg_data = httpConnection.fetch(table_name, keyspace, start_date, end_date, payload.toString(),
                hsqldbConnection);
        JSONArray final_data = new JSONArray();
        for(Object column_obj: bg_data){
            JSONObject cols = (JSONObject) column_obj;

            if (cols.has("result")){
//                System.out.println("RESULT IS HERE");
                result_array = (JSONArray) cols.get("result");
                for (Object col_obj : result_array){
                    JSONObject result_cols = (JSONObject) col_obj;
                    result_cols.put("id", UUID.randomUUID().toString());
                    final_data.put(result_cols);
                }

            }
            if (cols.has("site_code")) {
                site_check = (JSONArray) cols.get("site_code");
                if (site_check.isEmpty()) {
                    System.out.println("SITES DO NOT EXIST");
                    site_there = false;
                } else {
                    System.out.println("Adding Existing sites to HSQLDB:" + site_check);
//            hsqldbConnection.executeUpdateQuery("UPDATE V_SITESALE SET FINAL_DATE='" + start_date + "' where SITE_CODE = "+ site_check.get(Integer.parseInt("site_exist")) +"");
                    cols.remove("site_exist");
                    site_there = true;
                    System.out.println(cols.get("site_code"));
                    System.out.println("SITE_CODE HERE");
                }
            }
        }
        return final_data;
    }
}
