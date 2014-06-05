package com.datasift.dropwizard.examples.kafka;

import com.sun.istack.internal.NotNull;
import io.dropwizard.Configuration;

import java.util.Properties;

/**
 * Created by ram on 6/4/14.
 */
public class KafkaEnricherConfiguration extends Configuration{
    public JsonMap jsonMap;
    public GeoIp geoIp;
    public Producer producer;
    public class JsonMap {
        public String path;
        public String file;

        public Properties asProperties() {
            Properties p;
            p = new Properties();
            p.put("file",file);
            p.put("path",path);
            return p;
        }
    }
    public class GeoIp{
        public String path;
        public String ip;
        public String ipAsNum;
        public String ipAsNumV6;
        public String ipV6;
        public String liteCity;
        public String liteCityV6;

        public Properties asProperties(){
            Properties p = new Properties();
            p.put("path",path);
            p.put("ip",ip);
            p.put("ipASNum", ipAsNum);
            p.put("ipAsNumV6",ipAsNumV6);
            p.put("IpV6",ipV6);
            p.put("liteCity",liteCity);
            p.put("liteCityV6",liteCityV6);
            return p;
        }
    }
    public class Producer{
        @NotNull
        public String topic;
        public String test;
        public Properties asProperties(){
            Properties p = new Properties();
            p.put("topic",topic);
            return p;
        }
    }
}
