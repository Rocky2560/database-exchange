package com.ekbana.db.concrete_classes;

import com.ekbana.db.connections.DBConnection;
import com.ekbana.db.interfaces.databases;
import org.apache.log4j.Logger;
import org.json.JSONArray;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Postgres implements databases {
    private Connection conn;
    private static final Logger logger = Logger.getLogger(Postgres.class);
    private String url;
    private String user;
    private String pass;
    private DBConnection dbConnection;

    public Postgres(String url, String user, String pass){
        this.url = url;
        this.user = user;
        this.pass = pass;
        this.dbConnection = new DBConnection();
    }

    @Override
    public Connection connect() {
        this.conn = this.dbConnection.getDbConnection(conn,this.url,this.user,this.pass);
        return this.conn;
    }

    @Override
    public void store(List<PreparedStatement> psList) {
//        try {
//            BufferedWriter bf = new BufferedWriter(new FileWriter("/home/saurab/test_postgres"));
//            bf.write(psList.toString());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        System.exit(0);
        for(PreparedStatement ps: psList){
            try {
                ps.execute();
            } catch (SQLException e) {
               throw new RuntimeException(e);
            }
        }
    }

    @Override
    public ArrayList fetch() {
        return null;
    }

    public ResultSet fetchResultset() {
        return this.dbConnection.getResultSet(this.conn);
    }

}
