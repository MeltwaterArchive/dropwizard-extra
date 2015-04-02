package com.datasift.dropwizard.examples.test.kafka;

import com.datasift.dropwizard.examples.kafka.KafkaEnricherConfiguration;
import com.datasift.dropwizard.examples.kafka.MessageEnricher;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.io.Resources;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.io.FileInputStream;

/**
 * Created by ram on 6/4/14.
 */
public class MessageEnricherTest {

    private KafkaEnricherConfiguration config;
    private MessageEnricher enricher;
    @Before
    public void setup() throws Exception{
        final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        config = new ConfigurationFactory<>(KafkaEnricherConfiguration.class, validator, Jackson.newObjectMapper(), "dw")
                .build(new File(System.getProperty("user.dir")+File.separator+"kafka-service.yml"));
        enricher = new MessageEnricher(config);
    }

    @Test
    public void testEnricherTestCases() throws Exception{
        ObjectMapper mapper = new ObjectMapper();
        FileInputStream fis = new FileInputStream(new File(Resources.getResource("MessageEnricherTestCases.json").toURI()));
        JsonNode root = mapper.readTree(fis);
        JsonNode testCases = root.get("testCases");
        assertTrue("The returned type for testCases is not an array", testCases.getNodeType()==JsonNodeType.ARRAY);
        if(testCases.getNodeType() == JsonNodeType.ARRAY){
            for(int i=0; i<testCases.size(); i++){
                JsonNode testCase = testCases.get(i);
                assertTrue("The returned type for testCase is not an Object", testCase.getNodeType()==JsonNodeType.OBJECT);
                String input = testCase.get("input").toString();
                String output = testCase.get("output").toString();
                //System.out.println(input);
                String got = enricher.enrich(input);
                assertEquals("Failed to get the expected JSON object", output, got);
            }
        }
    }

}
