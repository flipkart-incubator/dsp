package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.models.misc.PartitionDetailsEmailNotificationRequest;
import com.flipkart.dsp.utils.EmailNotificationHelper;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.mail.EmailException;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 * +
 */
@Slf4j
@Api("email_notifications")
@Path("/v1/email_notifications")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class EmailNotificationResource {

    private final EmailNotificationHelper emailNotificationHelper;

    @POST
    @Path("/partition_state_change/send")
    @Timed
    @UnitOfWork
    public Response sendPartitionStateChangeEmail(PartitionDetailsEmailNotificationRequest partitionDetailsEmailNotificationRequest) throws EmailException {
        emailNotificationHelper.sendPartitionStateChangeEmail(partitionDetailsEmailNotificationRequest);
        return Response.ok().type(APPLICATION_JSON).build();
    }
}
