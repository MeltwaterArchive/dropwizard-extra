package com.datasift.dropwizard.bundles;

import com.datasift.dropwizard.jersey.inject.scala.CollectionsQueryParamInjectableProvider;
import com.yammer.dropwizard.Bundle;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;

/**
 * Bundle that provides Scala support to core functionality of Dropwizard Services.
 */
public class ScalaBundle implements Bundle {

    @Override
    public void initialize(final Bootstrap<?> bootstrap) {
        bootstrap.getObjectMapperFactory().registerModule(new DefaultScalaModule());
    }

    @Override
    public void run(final Environment environment) {
        environment.addProvider(new CollectionsQueryParamInjectableProvider());
    }
}
