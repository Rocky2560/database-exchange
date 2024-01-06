package com.ekbana.db.connections;

import java.sql.*;

public  class DBConnection {
    public ResultSet getResultSet(Connection conn) {
        ResultSet rs = null;
        try {
            Statement stmt = conn.createStatement();
            rs = stmt.executeQuery("");

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rs;
    }

    public Connection getDbConnection(Connection conn, String url, String user, String pass) {
        try {
            if (conn != null) {
                System.out.println();
            } else {
                conn = DriverManager.getConnection(url, user, pass);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return conn;
    }
}
