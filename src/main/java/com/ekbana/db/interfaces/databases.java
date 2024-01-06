package com.ekbana.db.interfaces;

import org.json.JSONArray;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

public interface databases {
    Connection connect();

    void store(List<PreparedStatement> ps);

    ArrayList fetch();

}
