package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.NotificationPreferencesActor;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.flipkart.dsp.utils.Constants.APPLICATION_JSON;

@Slf4j
@Api("notification_preference")
@Path("/v2/notification_preference")
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class NotificationPreferenceResource {

    private final NotificationPreferencesActor notificationPreferencesActor;

    @GET
    @Timed
    @UnitOfWork
    @Path("{workflow_id}")
    public Response getNotificationPreference(@PathParam("workflow_id") long workflowId) {
        return Response.ok().type(APPLICATION_JSON).entity(notificationPreferencesActor.getNotificationPreference(workflowId)).build();
    }

}
