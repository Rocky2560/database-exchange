package com.ekbana.db.connections;

import com.ekbana.db.bootstrap.Launcher;
import com.ekbana.db.concrete_classes.HSQLDBConnection;
import com.ekbana.db.config.ConnectorConfig;
import com.ekbana.db.dbencrypt.AES;
import com.ekbana.db.util.GetDate;
import org.apache.log4j.Logger;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import spark.InsertIntoCassandra;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;

public class HttpConnection {

    private static final Logger logger = Logger.getLogger(HttpConnection.class);
    private ConnectorConfig connectorConfig = Launcher.connector_config;
    private HttpURLConnection con;
    private GetDate getDate = new GetDate();

    public HttpConnection(String fetch_url) {
        URL url;
        try {
            url = new URL(fetch_url);
            con = (HttpURLConnection) url.openConnection();
        } catch (IOException e) {
            logger.error(e);
        }
    }

    public JSONArray fetch(String table_name, String keyspace, String start_date, String end_date, String payload,
                           HSQLDBConnection hsqldbConnection) {

        JSONArray data = null;
        try {

            if(con == null){
                logger.info("Connection is Null");
            }

            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("username", connectorConfig.getBg_user());
            con.setRequestProperty("password", connectorConfig.getBg_pass());
            con.setRequestProperty("Accept", "text/plain");
            con.setConnectTimeout(3600000);
            con.setReadTimeout(3600000);
            con.setDoOutput(true);

            writeToOutputStream(payload);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();

            data = new JSONArray(AES.decrypt(content.toString(), connectorConfig.getDecrypt_key()));

            if(data.length() > 0){
                try {
                    BufferedWriter bf = new BufferedWriter(new FileWriter(connectorConfig.getCount_file() + table_name+".json",true));
                    bf.write("{\"count\":"+data.length()+",\"start_date\":\""+start_date+"\",\"end_date\":\""+end_date+"\"}\n");
                    bf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                String count_record = "{" +
                        "\"created_at\":\"" + LocalDate.now() + "\"," +
                        "\"count\":" + data.length() + "," +
                        "\"start_date\":\"" + LocalDate.parse(start_date, DateTimeFormat.forPattern("dd-MMM-yy")) + "\"" +
                        "}";

                new InsertIntoCassandra().insertCount(count_record, table_name+"_count", keyspace);
                hsqldbConnection.executeUpdateQuery("UPDATE " + table_name.toUpperCase() + " SET FINAL_DATE='" + end_date + "'");
            }

        } catch (IOException e) {
            logger.error(e);
        }

        return data;
    }

    private void writeToOutputStream(String payload) throws IOException {
        try (OutputStream out = con.getOutputStream()) {
            byte[] input = payload.getBytes("utf-8");
            out.write(input, 0, input.length);
            out.flush();
            out.close();
        }
    }

    public JSONArray fetch(String table_name, int start_offset, String payload, HSQLDBConnection hsqldbConnection) {
        try {
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("username", connectorConfig.getBg_user());
            con.setRequestProperty("password", connectorConfig.getBg_pass());
            con.setRequestProperty("Accept", "application/json");
            con.setConnectTimeout(3600000);
            con.setReadTimeout(3600000);
            con.setDoOutput(true);

            writeToOutputStream(payload);

            JSONObject data_fetched = new JSONObject(new JSONTokener(new InputStreamReader((InputStream) con.getContent())));

            JSONArray data = new JSONArray(AES.decrypt(data_fetched.getString("value"), connectorConfig.getDecrypt_key()));

            int last_offset = data_fetched.getInt("offset_value");

            if(data.length() > 0 && last_offset > start_offset){
                try {
                    BufferedWriter bf = new BufferedWriter(new FileWriter(connectorConfig.getCount_file() + table_name+".json",true));
                    bf.write("{\"start_offset\":\""+start_offset+"\"," +
                            "\"last_offset\":\""+last_offset+"\"}\n");
                    bf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                hsqldbConnection.executeUpdateQuery("UPDATE " + table_name.toUpperCase() + " SET LAST_OFFSET=" + last_offset);
                return data;
            }else{
                logger.error("data length: " + data.length() + ", start_offset: " + start_offset + ", last_offset: " + last_offset);
            }
        } catch (IOException e) {
            logger.error(e);
        }
        return null;
    }

    public void shutdownConnection() {
        if (this.con != null) {
            this.con.disconnect();
        }
    }

}
