package com.ekbana.db.concrete_classes;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.ekbana.db.bootstrap.Launcher;
import com.ekbana.db.config.ConnectorConfig;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONObject;
import spark.InsertIntoCassandra;

public class Cassandra {

    private JSONObject finalResult;
    private Session cassandra_session;
    private ConnectorConfig connectorConfig = Launcher.connector_config;

    private Logger logger = Logger.getLogger(Cassandra.class);


    private Session initializeConnection() {
        JavaSparkContext sc = getJavaSparkContext();

        CassandraConnector connector = CassandraConnector.apply(sc.getConf());

        try {
            cassandra_session = connector.openSession();
        }
        catch (Exception e) {
            e.printStackTrace();
//            IOException is thrown due to NoHostAvailableException
//            but cannot use IOException as it is not thrown by the code explicitly
            logger.error("cassandra host: " + e.getMessage());
        }
        return cassandra_session;
    }

    private JavaSparkContext getJavaSparkContext(){
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = null;
        try {
            conf.setAppName("Spark Cassandra");
            conf.setMaster(connectorConfig.getSpark_master());
            conf.set("spark.cassandra.connection.host", connectorConfig.getCassandra_host());
            conf.set("spark.cassandra.connection.port", connectorConfig.getCassandra_port());
            conf.set("spark.cassandra.auth.username", connectorConfig.getCassandra_user());
            conf.set("spark.cassandra.auth.password", connectorConfig.getCassandra_pass());
            conf.set("spark.driver.allowMultipleContexts", "true");
            sc = new JavaSparkContext(conf);
        }catch (Exception e){
            logger.error(e.getMessage());
        }
        return sc;
    }

}
