package com.ekbana.db.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.util.List;

public class ConnectorConfig {

    public Config config = ConfigFactory.parseFile(new File("/etc/bigmart_datadump/app.conf"));

    private String postgres_url;
    private String postgres_user;
    private String postgres_pass;

    private String spark_master;
    private String cassandra_host;
    private String cassandra_port;
    private String cassandra_user;
    private String cassandra_pass;

    private String hsql_host;
    private String hsql_db;
    private String hsql_user;
    private String hsql_pass;

    private String bg_user;
    private String bg_pass;

    private String count_file;

    private String encrypt_key;
    private String decrypt_key;

    private String TRUSTSTORE_PATH;
    private String TRUSTSTORE_PASSWORD;
    private String KEYSTORE_PATH;
    private String KEYSTORE_PASSWORD;

    public ConnectorConfig(){
        this.postgres_url = config.getString("postgres_url");
        this.postgres_user = config.getString("postgres_user");
        this.postgres_pass = config.getString("postgres_pass");

        this.spark_master = config.getString("spark_master");
        this.cassandra_host = config.getString("cassandra_host");
        this.cassandra_port = config.getString("cassandra_port");
        this.cassandra_user = config.getString("cassandra_user");
        this.cassandra_pass = config.getString("cassandra_pass");

        this.hsql_host = config.getString("hsql_host");
        this.hsql_db = config.getString("hsql_db");
        this.hsql_user = config.getString("hsql_user");
        this.hsql_pass = config.getString("hsql_pass");

        this.bg_user = config.getString("bg_user");
        this.bg_pass = config.getString("bg_pass");

        this.count_file = config.getString("count_file");

        this.encrypt_key = config.getString("encrypt_key");
        this.decrypt_key = config.getString("decrypt_key");

        this.TRUSTSTORE_PATH = config.getString("TRUSTSTORE_PATH");
        this.TRUSTSTORE_PASSWORD = config.getString("TRUSTSTORE_PASSWORD");
        this.KEYSTORE_PATH = config.getString("KEYSTORE_PATH");
        this.KEYSTORE_PASSWORD = config.getString("KEYSTORE_PASSWORD");
    }

    public String getBg_user() {
        return bg_user;
    }

    public String getBg_pass() {
        return bg_pass;
    }

    public String getPostgres_url() {
        return postgres_url;
    }

    public String getPostgres_user() {
        return postgres_user;
    }

    public String getPostgres_pass() {
        return postgres_pass;
    }

    public String getSpark_master() {
        return spark_master;
    }

    public String getCassandra_host() {
        return cassandra_host;
    }

    public String getCassandra_port() {
        return cassandra_port;
    }

    public String getCassandra_user() {
        return cassandra_user;
    }

    public String getCassandra_pass() {
        return cassandra_pass;
    }

    public String getHsql_host() {
        return hsql_host;
    }

    public String getHsql_db() {
        return hsql_db;
    }

    public String getHsql_user() {
        return hsql_user;
    }

    public String getHsql_pass() {
        return hsql_pass;
    }

    public String getCount_file() {
        return count_file;
    }

    public String getEncrypt_key() {
        return encrypt_key;
    }

    public String getDecrypt_key() {
        return decrypt_key;
    }

    public String getTRUSTSTORE_PATH() {
        return TRUSTSTORE_PATH;
    }

    public String getTRUSTSTORE_PASSWORD() {
        return TRUSTSTORE_PASSWORD;
    }

    public String getKEYSTORE_PATH() {
        return KEYSTORE_PATH;
    }

    public String getKEYSTORE_PASSWORD() {
        return KEYSTORE_PASSWORD;
    }
}
