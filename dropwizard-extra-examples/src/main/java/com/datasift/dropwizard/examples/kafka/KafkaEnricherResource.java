package com.datasift.dropwizard.examples.kafka;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;

/**
 * Created by ram on 5/27/14.
*/
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class KafkaEnricherResource {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(KafkaEnricherResource.class);

    public KafkaEnricherResource() {

    }

    @GET
    public Response produce(@Context UriInfo uriInfo){
        // do nothing
        LOGGER.info(String.format("Trying to access / on KafkaEnricherResource not supported : %s", uriInfo.getRequestUri()));
        Response resp = Response.status(Response.Status.FORBIDDEN).build();
        return resp;
    }
}