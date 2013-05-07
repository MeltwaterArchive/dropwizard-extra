package com.datasift.dropwizard.bundles;

import com.datasift.dropwizard.jersey.dispatch.OptionResourceMethodDispatchAdapter;
import com.datasift.dropwizard.jersey.inject.scala.CollectionsQueryParamInjectableProvider;
import com.codahale.dropwizard.Bundle;
import com.codahale.dropwizard.setup.Bootstrap;
import com.codahale.dropwizard.setup.Environment;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import com.codahale.dropwizard.jersey.setup.JerseyEnvironment;

/**
 * Bundle that provides Scala support to core functionality of Dropwizard Services.
 */
public class ScalaBundle implements Bundle {

    @Override
    public void initialize(final Bootstrap<?> bootstrap) {
        bootstrap.getObjectMapper().registerModule(new DefaultScalaModule());
    }

    @Override
    public void run(final Environment environment) {
        final JerseyEnvironment jersey = environment.jersey();
        jersey.addProvider(new CollectionsQueryParamInjectableProvider());
        jersey.addProvider(new OptionResourceMethodDispatchAdapter());
    }
}
