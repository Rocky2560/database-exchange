package com.ekbana.db.dbencrypt;

import com.ekbana.db.bootstrap.Launcher;
import com.ekbana.db.config.ConnectorConfig;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PrepareQuery {
    private Logger log = LoggerFactory.getLogger(PrepareQuery.class);
    private List<String> column_mask;
    private String query;
    private ConnectorConfig connectorConfig = Launcher.connector_config;
    private PreparedStatement ps;

    public void formUpsert(final String tableName, List<String> column_mask) {
//        this.column_mask = column_mask;
//        System.out.println(column_mask);
//        System.out.println();
//
//        StringBuilder query = new StringBuilder("insert into " + tableName + " (");
//        StringBuilder query_part2 = new StringBuilder(") values (");
//
//        for(String column_name: column_mask){
//            query.append("" + column_name + "," + column_name + "_en,");
//            query_part2.append("?,?,");
//        }
//
//        query.deleteCharAt(query.lastIndexOf(","));
//        query_part2.deleteCharAt(query_part2.lastIndexOf(","));
//        query.append(query_part2).append(")");
//        log.debug("Query formed " + query);
//        this.query = query.toString();

        this.query = "insert into " + tableName + " (barcode, barcode_en, billno, billno_en, cat1, cat1_en, cat2, cat2_en, " +
                "department, department_en, division, division_en, icode, icode_en, lpcardno, lpcardno_en, section, " +
                "section_en) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    }

    public List<PreparedStatement> execute(Connection connection, List<Map<String, Object>> encrypted_columns_list) {

        List<PreparedStatement> psList = new ArrayList<>();

        try {
            //Iterate over fields

            for (Map<String, Object> encrypted_columns : encrypted_columns_list) {
                 ps = connection.prepareStatement(query);
                int paramIndex = 1;
                for (Map.Entry<String, Object> keys : encrypted_columns.entrySet()) {
                    String field = keys.getKey();
                    Object value = keys.getValue();
                    if (field.contains("_en")) {
                        addColumntoPreparedStatement("string", value, paramIndex++);
                    } else {
                        String schema_type = connectorConfig.config.getString("v_ekb_cust_sale.schema." + field);
                        addColumntoPreparedStatement(schema_type, value, paramIndex++);
                    }
                }
                psList.add(ps);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return psList;
    }

    private void addColumntoPreparedStatement(String type, Object value, int paramIndex) {
        try {
            switch (type) {
                case "string": {
                    if (value != null) {
                        ps.setString(paramIndex, String.valueOf(value));
//                        System.out.println(value);
                    } else
                        ps.setNull(paramIndex, Types.VARCHAR);
                }
                break;
                case "boolean": {
                    if (value != null) {
                        ps.setBoolean(paramIndex, Boolean.getBoolean(String.valueOf(value)));
                    } else {
                        ps.setNull(paramIndex, Types.BOOLEAN);
                    }
                }
                break;
                case "bytes": {
                    if (value != null) {
                        ps.setBytes(paramIndex, DatatypeConverter.parseBase64Binary((String) value));
                    } else {
                        ps.setNull(paramIndex, Types.BINARY);
                    }
                }
                break;
                case "float32":
                case "float64": {
                    if (value != null) {
                        ps.setDouble(paramIndex, Double.valueOf(String.valueOf(value)));
                    } else {
                        ps.setNull(paramIndex, Types.FLOAT);
                    }
                }
                break;
                case "int8":
                case "int16":
                case "int32": {
                    if (value != null) {
                        ps.setLong(paramIndex, Long.valueOf(String.valueOf(value)));
                    } else {
                        ps.setNull(paramIndex, Types.BIGINT);
                    }
                }
                break;
                case "int64": {
                    if (value != null) {
                        System.out.println("setting value to timestamp type");
                        System.out.println(java.sql.Timestamp.valueOf(String.valueOf(value)));
                        ps.setTimestamp(paramIndex, java.sql.Timestamp.valueOf(String.valueOf(value)));
                    } else {
                        ps.setNull(paramIndex, Types.TIMESTAMP);
                    }
                }
                break;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
