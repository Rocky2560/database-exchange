package com.ekbana.db.bootstrap;

import com.ekbana.db.concrete_classes.HSQLDBConnection;
import com.ekbana.db.config.ConnectorConfig;
import com.ekbana.db.tables.*;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import spark.CreateDF;
import spark.CreateSparkConnection;


public class Launcher {
    private Logger logger = Logger.getLogger(Launcher.class);
    public static final ConnectorConfig connector_config = new ConnectorConfig();
    public static final SparkSession sp = new CreateSparkConnection().cassandra_spark();

    public static void main(String[] args) {
        new Launcher();
    }

    private Launcher() {
        try {
            String request = System.getProperty("request");
            int request_type = 0;
            if (request != null) {
                request_type = Integer.parseInt(System.getProperty("request"));
            }
            String runtime_msg = "required request type:" +
                    "\nrequest: 1/2/3/4/5" +
                    "\n1: Hash sales table with start_date and final_date provided; default date=1 day interval" +
                    "\n2: Encrypt sales table with start_date and final_date provided; default date=1 day interval" +
                    "\n3: Encrypt collection table with start_date and final_date provided; default date=1 day interval" +
                    "\n4: Encrypt customer table with offset provided; default offset=0" +
                    "\n5: Encrypt sites table with offset provided; default offset=0" +
                    "\n6: Encrypt items table with offset provided; default offset=0";

            if (request_type == 0) {
                throw new RuntimeException(runtime_msg);
            }

            switch (request_type) {
                case 1:
                    throw new RuntimeException("Currently hashing service is disabled!!!");
                case 2:
                case 3:
                    new Sales().storeSalesData(request_type);
                    break;
                case 4:
                    new Customer().storeCustomerData();
                    break;
                case 5:
                    new Site().storeSitesData();
                    break;
                case 6:
                    new Item().storeItemsData();
                    break;
                case 7:
//                    System.out.println("entering case 7");
                    new SalesSite().storeSalesData(request_type);
                    break;
                case 8:
                    new CreateDF().read_from_shql();
                    break;
                case 9:
                new HSQLDBConnection().fetchSiteCode1();
//                    new HSQLDBConnection().fetchFinalDate("V_SITESALE");
                    break;
                default:
                    throw new RuntimeException("Unknown request type!\n" + runtime_msg);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
