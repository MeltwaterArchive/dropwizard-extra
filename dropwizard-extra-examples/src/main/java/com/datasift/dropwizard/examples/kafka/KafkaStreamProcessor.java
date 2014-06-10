package com.datasift.dropwizard.examples.kafka;

import ch.qos.logback.classic.Logger;
import com.datasift.dropwizard.kafka.consumer.StreamProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import kafka.consumer.ConsumerIterator;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.slf4j.LoggerFactory;


/**
 * Created by ram on 6/2/14.
 */
public class KafkaStreamProcessor implements StreamProcessor {
    private Producer producer;
    private MessageEnricher enricher;
    private KafkaEnricherConfiguration kec;
    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(KafkaStreamProcessor.class);
    public KafkaStreamProcessor(Producer producer, KafkaEnricherConfiguration kec, MessageEnricher enricher){
        this.producer = producer;
        this.enricher = enricher;
        this.kec = kec;
    }

    @Override
    public void process(Iterable stream, String topic) {
        ConsumerIterator<byte[], byte[]> it = (ConsumerIterator<byte[], byte[]>)stream.iterator();
        while (it.hasNext()) {
            //System.out.println(new String(it.next().message()));
            String msg = new String(it.next().message());
            LOGGER.info(String.format("Enriching Message: %s",msg));
            msg = enricher.enrich(msg);
            LOGGER.info(String.format("Enriched Message: %s", msg));
            //KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(kec.kafkaProducer.topic, "message", msg);
           // producer.send(keyedMessage);
        }
    }

}
