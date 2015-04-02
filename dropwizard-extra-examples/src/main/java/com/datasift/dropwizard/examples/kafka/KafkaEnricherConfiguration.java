package com.datasift.dropwizard.examples.kafka;

import com.datasift.dropwizard.kafka.KafkaConsumerFactory;
import com.datasift.dropwizard.kafka.KafkaProducerFactory;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

import java.util.Properties;

/**
 * Created by ram on 6/4/14.
 */
public class KafkaEnricherConfiguration extends Configuration{
    @JsonProperty("json.map")
    private JsonMap jsonMap;

    public JsonMap getJsonMap() {
        return jsonMap;
    }

    public void setJsonMap(JsonMap jsonMap) {
        this.jsonMap = jsonMap;
    }


    @JsonProperty("geo.ip")
    private GeoIp geoIp;

    public GeoIp getGeoIp() {
        return geoIp;
    }

    public void setGeoIp(GeoIp geoIp) {
        this.geoIp = geoIp;
    }

    @JsonProperty("kafka.producer.topic")
    private String producerTopic;

    public String getProducerTopic() {
        return producerTopic;
    }

    public void setProducerTopic(String producerTopic) {
        this.producerTopic = producerTopic;
    }


    //public Producer producer;
    protected static class JsonMap {

        private String file;

        public String getFile() {
            return file;
        }

        public void setFile(String file) {
            this.file = file;
        }

        private String path;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public Properties asProperties() {
            Properties p;
            p = new Properties();
            p.put("file",file);
            p.put("path",path);
            return p;
        }
    }
    protected static class GeoIp{
        private String path;
        private String ip;
        private String ipAsNum;
        private String ipAsNumV6;
        private String ipV6;
        private String liteCity;
        private String liteCityV6;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getIpAsNum() {
            return ipAsNum;
        }

        public void setIpAsNum(String ipAsNum) {
            this.ipAsNum = ipAsNum;
        }

        public String getIpAsNumV6() {
            return ipAsNumV6;
        }

        public void setIpAsNumV6(String ipAsNumV6) {
            this.ipAsNumV6 = ipAsNumV6;
        }

        public String getIpV6() {
            return ipV6;
        }

        public void setIpV6(String ipV6) {
            this.ipV6 = ipV6;
        }

        public String getLiteCity() {
            return liteCity;
        }

        public void setLiteCity(String liteCity) {
            this.liteCity = liteCity;
        }

        public String getLiteCityV6() {
            return liteCityV6;
        }

        public void setLiteCityV6(String liteCityV6) {
            this.liteCityV6 = liteCityV6;
        }

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

    @JsonProperty("kafka.consumer")
    public KafkaConsumerFactory kafkaConsumer = new KafkaConsumerFactory();

    public KafkaConsumerFactory getKafkaConsumerFactory() {
        return kafkaConsumer;
    }

    public void setKafkaConsumerFactory(KafkaConsumerFactory factory) {
        this.kafkaConsumer = factory;
    }


    @JsonProperty("kafka.producer")
    public KafkaProducerFactory kafkaProducer = new KafkaProducerFactory();

    public KafkaProducerFactory getKafkaProducerFactory() {
        return kafkaProducer;
    }

    public void setKafkaProducerFactory(KafkaProducerFactory kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

/*
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
*/
}
