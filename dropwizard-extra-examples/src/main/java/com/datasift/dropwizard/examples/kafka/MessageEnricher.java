package com.datasift.dropwizard.examples.kafka;

import ch.qos.logback.classic.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;

import com.maxmind.geoip.LookupService;
import com.maxmind.geoip.Location;

import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

/**
 * Created by ram on 6/4/14.
 */
public class MessageEnricher {
    private LookupService lookupService;
    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(MessageEnricher.class);
    private KafkaEnricherConfiguration kec;
    private JsonNode rootNode;
    private JsonNode keyMap;
    private static final String USER_IP = "user_ip";

    public MessageEnricher(KafkaEnricherConfiguration kec) throws Exception{
        this.kec = kec;
        rootNode = loadMapFile();
        KafkaEnricherConfiguration.GeoIp geoIp = kec.getGeoIp();
        String geoIpPath = geoIp.getPath();
        if(geoIpPath.equals(".")){
            geoIpPath = System.getProperty("user.dir");
            geoIp.setPath(geoIpPath);
        }
        lookupService = new LookupService(geoIpPath+ File.separator+geoIp.getLiteCityV6(),
                LookupService.GEOIP_MEMORY_CACHE );
    }
    private static String getStackTrace(Throwable t){
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        return sw.toString();
    }

    private JsonNode loadMapFile() throws Exception{

        KafkaEnricherConfiguration.JsonMap jsonMap = kec.getJsonMap();
        String file = jsonMap.getFile();
        String path = jsonMap.getPath();
        if(path.equals(".")){
            path = System.getProperty("user.dir");
        }
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(new File(path+File.separator+file));
    }
    private String getFullName(String abbr, JsonNode keyMap){
        JsonNode nameNode = keyMap.path(abbr);
        JsonNodeType type = nameNode.getNodeType();
        String rep = abbr;
        //if(type == ){
        switch (type) {
            case STRING:
                rep = nameNode.textValue();
                break;
            case ARRAY:
                Iterator<JsonNode> iter = nameNode.elements();
                while(iter.hasNext()){
                    JsonNode node = iter.next();
                    rep = node.textValue();
                    break;
                }
                break;
            case OBJECT:
            case BINARY:
            case BOOLEAN:
            case MISSING:
            case NULL:
            case NUMBER:
            case POJO:
            default:
                LOGGER.warn(String.format("%s unknown JsonNodeType %s", abbr, type.toString()));
                break;
        }
        return rep;
    }
    private Map<String,String> replaceKeyName(Map<String, String> in, Map<String,String> out, String key, JsonNode keyMap){
        String fn = getFullName(key, keyMap);
        //make sure that the full name is not identical to the key
        if(!fn.equals(key)){

            //fetch the value
            String val = in.get(key);
            if(fn.indexOf("_encoded") != -1){

                //base64 decode the value
                String decodedVal = StringUtils.newStringUtf8(Base64.decodeBase64(val));
                try {
                    LOGGER.info(String.format("The decoded value: %s", decodedVal));
                    Map<String, String> myMap = new HashMap<String, String>();
                    ObjectMapper objectMapper = new ObjectMapper();
                    myMap = objectMapper.readValue(decodedVal, HashMap.class);

                    out.putAll(myMap);
                }catch (IOException ex){
                    LOGGER.error(String.format("Could not convert value(%s) of key(%s) to JSON. Error: %s", decodedVal, key, ex.getMessage()));
                }
            }else if (fn.equals("platform") ||fn.equals("event")){
                /*
                 "valid_values": {
                    "event": {
                      "ad": "ad_impression",
                      "iv": "item_view",
                      "lc": "link_click",
                      "pp": "page_ping",
                      "pv": "page_view",
                      "se": "custom_structured_event",
                      "stk": "social_tracking",
                      "tr": "transaction_tracking",
                      "ue": "custom_unstructured_event"
                    },
                    "platform": {
                      "app": "general_app",
                      "cnsl": "games_console",
                      "iot": "internet_of_things",
                      "mob": "mobile",
                      "pc": "desktop_laptop_notebook",
                      "srv": "server_side_app",
                      "tv": "connected_tv",
                      "web": "website"
                    }
                 }
                */
                //test the values to valid
                JsonNode validValues = rootNode.path("valid_values");
                JsonNode keyValidValues = validValues.path(fn);
                JsonNode validVal = keyValidValues.get(val);
                if(validVal !=null){
                    val = validVal.textValue();
                }else{
                    LOGGER.warn(String.format("Found unknown value(%s) for event(%s)", val, fn));
                }
                out.put(fn, val);
            }else {
                //add new key with value
                out.put(fn, val);
            }
        }
        return out;
    }
    private Map<String, String> replaceKeys(Map<String, String> map, JsonNode keyMap){
        Iterator<String> keys = map.keySet().iterator();
        Map<String, String> out = new HashMap<String, String>();
        while (keys.hasNext()){
            String key = keys.next();
            out = replaceKeyName(map, out, key, keyMap);
        }
        return out;
    }
    public String enrich(String message){
        try {
            // Convert the message to JSON object
            Map<String, String> myMap = new HashMap<String, String>();
            ObjectMapper objectMapper = new ObjectMapper();
            myMap = objectMapper.readValue(message, HashMap.class);

            LOGGER.info("Un-abbreviating the keys in message " + message);
            //replace the abbreviated keys with fully qualified ones
            myMap = replaceKeys(myMap, keyMap);


            //add Geo Location from IP
            String ipAddress = myMap.get(USER_IP);
            if (ipAddress != null ) {
                if( lookupService != null) {
                    Location l1 = lookupService.getLocation(ipAddress);
                    if(l1 != null) {
                        myMap.put("ip_country_code", l1.countryCode);
                        myMap.put("ip_region", l1.region);
                        myMap.put("ip_city", l1.city);
                        myMap.put("ip_postalCode", l1.postalCode);
                        myMap.put("ip_latitude", Float.toString(l1.latitude));
                        myMap.put("ip_longitude", Float.toString(l1.longitude));
                        myMap.put("ip_metro_code", Integer.toString(l1.metro_code));
                        myMap.put("ip_area_code", Integer.toString(l1.area_code));
                    }else{
                        LOGGER.error(String.format("Location l1 is null for  IP address : %s. ", ipAddress));
                    }
                }else{
                    LOGGER.error(String.format("the GeoIp lookup service is null : %s. Can not test ip to geo for IP address %s ",lookupService, ipAddress));
                }
            } else {
                LOGGER.error(String.format("the IP address field is null : %s",ipAddress));
            }
            //StringWriter writer =  new StringWriter();
            message = objectMapper.writeValueAsString(myMap);
            // message = writer.toString();

        }catch(IOException ex){
            LOGGER.error(String.format("Failed to parse JSON String %s - %s \n %s", message, ex.getMessage(), getStackTrace(ex)));
        }
        return message;
    }
}
