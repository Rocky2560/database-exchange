package com.ekbana.db.factory;

import com.ekbana.db.bootstrap.Launcher;
import com.ekbana.db.concrete_classes.Oracle;
import com.ekbana.db.concrete_classes.Postgres;
import com.ekbana.db.config.ConnectorConfig;
import com.ekbana.db.interfaces.databases;

public class ConnectionFactory {
    private ConnectorConfig config = Launcher.connector_config;

    public databases getConnection(String ConnectionClass) {
        if (ConnectionClass == null) {
            return null;
        }
        if (ConnectionClass.equalsIgnoreCase("POSTGRES")) {
            return new Postgres(config.getPostgres_url(), config.getPostgres_user(), config.getPostgres_pass());

        } else if (ConnectionClass.equalsIgnoreCase("ORACLE")) {
            return new Oracle(null, null, null);

        }

        return null;
    }
}
