package com.datasift.dropwizard.examples.kafka;


import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {
    //what do we do with these properties??
    private VerifiableProperties props;

    public SimplePartitioner (VerifiableProperties props) {
       this.props = props;
    }

    public int partition(String key, int numPartitions) {
        int partition = 0;
        int offset = key.lastIndexOf('.');
        if (offset > 0) {
            partition = Integer.parseInt( key.substring(offset+1)) % numPartitions;
        }
        return partition;
    }

    @Override
    public int partition(Object o, int i) {
        if(o instanceof String){
            return partition((String)o, i);
        }else{
            //return the default partition
            return 0;
        }

    }
}