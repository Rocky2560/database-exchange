package com.ekbana.db.concrete_classes;

import com.ekbana.db.bootstrap.Launcher;
import com.ekbana.db.config.ConnectorConfig;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

public class HSQLDBConnection {

    private Logger logger = Logger.getLogger(HSQLDBConnection.class);
    private static ConnectorConfig connectorConfig = Launcher.connector_config;
    final String JDBC_DRIVER = "org.hsqldb.jdbc.JDBCDriver";
    private Connection conn;
    private String DB_URL = "";
    private String USER = "";
    private String PASS = "";

    public HSQLDBConnection() {
        this.DB_URL = "jdbc:hsqldb:hsql://"+connectorConfig.getHsql_host()+"/"+connectorConfig.getHsql_db();
        this.USER = connectorConfig.getHsql_user();
        this.PASS = connectorConfig.getHsql_pass();
    }

    public static String getDB_URL() {
        return "jdbc:hsqldb:hsql://"+connectorConfig.getHsql_host()+"/"+connectorConfig.getHsql_db();
    }

    public static String getUSER() {
        return connectorConfig.getHsql_user();
    }

    public static String getPASS() {
        return connectorConfig.getHsql_pass();
    }


    public void get_or_create_connection() {
        try {
            // STEP 1: Register JDBC driver
            Class.forName(JDBC_DRIVER);

            // STEP 2: Open a connection
            logger.info("Connecting to a selected database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

        } catch (SQLException | ClassNotFoundException e) {
            logger.error(e);
        }
    }

    public void executeUpdateQuery(String query){
        if(conn == null){
            get_or_create_connection();
        }

        Statement stmt;
        try {
            stmt = conn.createStatement();
            logger.info("query to execute: " + query);
            stmt.executeUpdate(query);
            conn.commit();
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    public String fetchLastFetchedDate(String table_name){
        if(conn == null){
            get_or_create_connection();
        }

        Statement stmt;
        String final_date = "";

        try {
            stmt = conn.createStatement();
            String query = "SELECT FINAL_DATE FROM " + table_name.toUpperCase();
            logger.info("query to execute: " + query);
            ResultSet rs = stmt.executeQuery(query);
            while(rs.next()){
                final_date = rs.getString("final_date");
            }
        } catch (SQLException e) {
            System.out.println("Error while getting sales final_date: " + e.getMessage());
        }
        return final_date;
    }

    public int fetchLastOffset(String table_name){
        if(conn == null){
            get_or_create_connection();
        }

        Statement stmt;
        int last_offset = 0;

        try {
            stmt = conn.createStatement();
            String query = "SELECT LAST_OFFSET FROM " + table_name.toUpperCase();
            logger.info("query to execute: " + query);
            ResultSet rs = stmt.executeQuery(query);
            while(rs.next()){
                last_offset = rs.getInt("last_offset");
            }
        } catch (SQLException e) {
            System.out.println("Error while getting the last offset: " + e.getMessage());
        }
        return last_offset;
    }

    public ArrayList fetchSiteCode(){
        if(conn == null){
            get_or_create_connection();
        }
        ArrayList store_sites = new ArrayList();
        Statement stmt;
        try {
            stmt = conn.createStatement();
            String query = "SELECT SITE_CODE FROM V_SITESALE";
            logger.info("query to execute: " + query);
            ResultSet rs = stmt.executeQuery(query);
            while(rs.next()){
                store_sites.add(rs.getObject(1));
            }
        } catch (SQLException e) {
            System.out.println("Error while getting sales final_date: " + e.getMessage());
        }
        return store_sites;
    }

    public JSONArray fetchSiteCode1(){
        if(conn == null){
            get_or_create_connection();
        }

        Statement stmt;
        String final_date = "";
        JSONArray ja = new JSONArray();
        Map<String,Object> group_site = new TreeMap<String, Object>();
        try {
            stmt = conn.createStatement();
            String query = "select final_date, group_concat(site_code) site_code from v_sitesale group by final_date";
            logger.info("query to execute: " + query);
            ResultSet rs = stmt.executeQuery(query);
//            ResultSetMetaData rsmd = null;
//            rsmd = rs.getMetaData();
//            int num_col = 0;
//            num_col = rsmd.getColumnCount();
            while(rs.next()){
                group_site.put("final_date", rs.getObject(1));
                group_site.put("site_code", rs.getObject(2));
//              final_date = rs.getString("final_date");
                ja.put(group_site);
            }

//            for(Object column_obj: ja){
//                JSONObject cols = (JSONObject) column_obj;
//                SimpleDateFormat format = new SimpleDateFormat("dd-MMM-yy");
//                Date date = format.parse(start_date);
//                String format_date = new SimpleDateFormat("yyyy-MM-dd").format(date);
////                System.out.println(new SimpleDateFormat("yyyy-MM-dd").format(date));
//                System.out.println(format_date);
//                if (cols.has("final_date") && cols.get("final_date").equals(format_date)) {
//                    final_date = start_date;
////                System.out.println(site_exist);
//                    System.out.println("YOURE IN");
//                }
//            }
        } catch (SQLException e) {
            System.out.println("Error while getting sales final_date: " + e.getMessage());
        }
        return ja;
    }

    public String fetchFinalDate(String table_name){
        if(conn == null){
            get_or_create_connection();
        }
        Statement stmt;
        Object final_date = "";
        try {
            stmt = conn.createStatement();
            String query = "SELECT max(FINAL_DATE) FROM " + table_name.toUpperCase();
            logger.info("query to execute: " + query);
            ResultSet rs = stmt.executeQuery(query);
            while(rs.next()){
                final_date = rs.getObject(1);
            }
        } catch (SQLException e) {
            System.out.println("Error while getting sales final_date: " + e.getMessage());
        }
//        System.out.println(final_date);
        return String.valueOf(final_date);
    }
}