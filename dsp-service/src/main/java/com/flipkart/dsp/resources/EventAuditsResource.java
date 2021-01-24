package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.EventAuditActor;
import com.flipkart.dsp.actors.RequestActor;
import com.flipkart.dsp.actors.WorkFlowActor;
import com.flipkart.dsp.config.DSPClientConfig;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.exception.DSPSvcException;
import com.flipkart.dsp.models.EventLevel;
import com.flipkart.dsp.models.event_audits.EventAudit;
import com.flipkart.dsp.models.event_audits.Events;
import com.flipkart.dsp.models.event_audits.event_type.sg_node.SGStartInfoEvent;
import com.flipkart.dsp.models.event_audits.event_type.wf_node.WFContainerCompletedInfoEvent;
import com.flipkart.dsp.models.event_audits.event_type.wf_node.WFContainerFailedEvent;
import com.flipkart.dsp.models.event_audits.event_type.wf_node.WFContainerStartedDebugEvent;
import com.flipkart.dsp.models.event_audits.event_type.wf_node.WFContainerStartedInfoEvent;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.EventAuditUtil;
import com.flipkart.dsp.utils.PathHelper;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Objects;


@Api("event_audits")
@Path("/v1/event_audit")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class EventAuditsResource {
    private final PathHelper pathHelper;
    private final RequestActor requestActor;
    private final WorkFlowActor workFlowActor;
    private final EventAuditUtil eventAuditUtil;
    private final DSPClientConfig dspClientConfig;
    private final EventAuditActor eventAuditsActor;

    @GET
    @Timed
    @UnitOfWork(readOnly = true)
    public List<EventAudit> getEvents(@QueryParam("offset") Long offset, @QueryParam("eventLevel") List<EventLevel> eventLevelList,
                                      @NotNull @QueryParam("requestId") Long requestId) throws com.flipkart.dsp.exception.DSPSvcException {
        validateRequestId(requestId);
        List<EventAudit> eventAuditList = eventAuditsActor.getEvents(offset, requestId, eventLevelList);
        preProcessEvents(eventAuditList);
        return eventAuditList;
    }

    private void validateRequestId(Long requestId) throws com.flipkart.dsp.exception.DSPSvcException {
        if (requestId == 0) throw new com.flipkart.dsp.exception.DSPSvcException("query param requestId can't be null");
        Request request = requestActor.getRequest(requestId);
        if (Objects.isNull(request))
            throw new com.flipkart.dsp.exception.DSPSvcException("Request not found for id: " + requestId);
    }

    private void validateWorkflowId(Long workflowId) throws DSPSvcException {
        Workflow workflow = workFlowActor.getWorkFlowById(workflowId);
        if (Objects.isNull(workflow))
            throw new DSPSvcException("WorkflowEntity not found for id: " + workflowId);
    }

    private void preProcessEvents(List<EventAudit> eventAuditsList) {
        String logPrefixPath = String.format(Constants.LOG_PATH_PREFIX, Constants.http,
                dspClientConfig.getHost(), dspClientConfig.getPort(), Constants.LOG_RESOURCE_PREFIX);
        eventAuditsList.forEach(eventAudit -> {
            Events events = eventAudit.getPayload();
            if ((events instanceof WFContainerCompletedInfoEvent)) {
                // processing for logurl
                WFContainerCompletedInfoEvent wfContainerCompletedInfoEvent = (WFContainerCompletedInfoEvent) events;
                wfContainerCompletedInfoEvent.setAttemptLogUrlMapping(eventAuditUtil.populateAbsoluteLogUrl(
                        wfContainerCompletedInfoEvent.getContainerDetails().getLogAttemptMap(), logPrefixPath));
            } else if (events instanceof WFContainerFailedEvent) {
                WFContainerFailedEvent wfContainerFailedEvent = ((WFContainerFailedEvent) events);
                wfContainerFailedEvent.setAttemptLogUrlMapping(eventAuditUtil.populateAbsoluteLogUrl(
                        wfContainerFailedEvent.getContainerDetails().getLogAttemptMap(), logPrefixPath));
            } else if (events instanceof WFContainerStartedDebugEvent) {
                WFContainerStartedDebugEvent wfContainerStartedDebugEvent = ((WFContainerStartedDebugEvent) events);
                wfContainerStartedDebugEvent.setLogAttemptUrlMapping(eventAuditUtil.populateAbsoluteLogUrl(
                        wfContainerStartedDebugEvent.getLogAttemptMap(), logPrefixPath));
                wfContainerStartedDebugEvent.setDataFrameWebHDFSLinkMapping(pathHelper.getDFHDFSCompletePath(
                        wfContainerStartedDebugEvent.getInputDetails()));
            } else if (events instanceof WFContainerStartedInfoEvent) {
                WFContainerStartedInfoEvent wfContainerStartedInfoEvent = ((WFContainerStartedInfoEvent) events);
                wfContainerStartedInfoEvent.setLogAttemptUrlMapping(eventAuditUtil.populateAbsoluteLogUrl(
                        wfContainerStartedInfoEvent.getLogAttemptMap(), logPrefixPath)
                );
            }else if (events instanceof SGStartInfoEvent) {
                SGStartInfoEvent sgStartInfoEvent = ((SGStartInfoEvent)events);
                sgStartInfoEvent.setLogUrl(eventAuditUtil.getSgLogUrl(sgStartInfoEvent.getLogUrl(), logPrefixPath));
            }
        });
    }

    @POST
    @Timed
    @UnitOfWork
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    public void saveEventAudit(@Valid EventAudit eventAudit) throws com.flipkart.dsp.exception.DSPSvcException {
        validateRequestId(eventAudit.getRequestId());
        validateWorkflowId(eventAudit.getWorkflowId());
        eventAuditsActor.saveEvent(eventAudit);
    }
}
