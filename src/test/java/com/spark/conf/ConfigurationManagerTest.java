package com.spark.conf;


import org.junit.Test;

public class ConfigurationManagerTest {

    @Test
    public void getProperty() throws Exception {
        String value1 = ConfigurationManager.getProperty("key1");
        String value2 = ConfigurationManager.getProperty("key2");
        System.out.println(value1 + " - " + value2);

    }

}
