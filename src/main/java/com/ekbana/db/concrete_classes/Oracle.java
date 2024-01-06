package com.ekbana.db.concrete_classes;

import com.ekbana.db.interfaces.databases;
import com.ekbana.db.connections.DBConnection;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Oracle implements databases {
    private Connection conn;
    private static final Logger logger = Logger.getLogger(Oracle.class);
    private String url;
    private String user;
    private String pass;
    private DBConnection dbConnection;

    public Oracle(String url, String user, String pass) {
        this.url = url;
        this.user = user;
        this.pass = pass;
    }

    @Override
    public Connection connect() {
        this.conn = this.dbConnection.getDbConnection(conn,this.url,this.user,this.pass);
        return this.conn;
    }

    @Override
    public void store(List<PreparedStatement> psList) {

    }

    @Override
    public ArrayList fetch() {
        return null;
    }

    public ResultSet fetchResultset() {
        return this.dbConnection.getResultSet(this.conn);
    }

}
