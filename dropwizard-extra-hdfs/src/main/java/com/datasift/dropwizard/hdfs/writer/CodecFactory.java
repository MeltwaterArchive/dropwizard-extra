package com.datasift.dropwizard.hdfs.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: justinhancock
 * Date: 01/11/2012
 * Time: 18:24
 * To change this template use File | Settings | File Templates.
 */
public class CodecFactory {
    private static Logger  LOG = LoggerFactory.getLogger(CodecFactory.class);
    public static CompressionCodec getCodec(String codecName) {
        Configuration conf = new Configuration();
        List<Class<? extends CompressionCodec>> codecs = CompressionCodecFactory
                .getCodecClasses(conf);
        // Wish we could base this on DefaultCodec but appears not all codec's
        // extend DefaultCodec(Lzo)
        CompressionCodec codec = null;
        ArrayList<String> codecStrs = new ArrayList<String>();
        codecStrs.add("None");
        for (Class<? extends CompressionCodec> cls : codecs) {
            codecStrs.add(cls.getSimpleName());
            if (codecMatches(cls, codecName)) {
                try {
                    codec = cls.newInstance();
                } catch (InstantiationException e) {
                    LOG.error("Unable to instantiate " + cls + " class");
                } catch (IllegalAccessException e) {
                    LOG.error("Unable to access " + cls + " class");
                }
            }
        }

        if (codec == null) {
            if (!codecName.equalsIgnoreCase("None")) {
                throw new IllegalArgumentException("Unsupported compression codec "
                        + codecName + ".  Please choose from: " + codecStrs);
            }
        } else if (codec instanceof org.apache.hadoop.conf.Configurable) {
            // Must check instanceof codec as BZip2Codec doesn't inherit Configurable
            // Must set the configuration for Configurable objects that may or do use
            // native libs
            ((org.apache.hadoop.conf.Configurable) codec).setConf(conf);
        }
        return codec;
    }
    private static boolean codecMatches(Class<? extends CompressionCodec> cls,
                                        String codecName) {
        String simpleName = cls.getSimpleName();
        if (cls.getName().equals(codecName)
                || simpleName.equalsIgnoreCase(codecName)) {
            return true;
        }
        if (simpleName.endsWith("Codec")) {
            String prefix = simpleName.substring(0,
                    simpleName.length() - "Codec".length());
            if (prefix.equalsIgnoreCase(codecName)) {
                return true;
            }
        }
        return false;
    }
}
