package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.ScriptActor;
import com.flipkart.dsp.api.ScriptAPI;
import com.flipkart.dsp.entities.script.Script;
import com.flipkart.dsp.entities.script.ScriptMeta;
import com.flipkart.dsp.exception.DSPSvcException;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 */
@Slf4j
@Api("script")
@Path("/v1/scripts")
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ScriptResource {
    private final ScriptAPI scriptAPI;
    private final ScriptActor scriptActor;

    @POST
    @Timed
    @UnitOfWork
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Long createScript(@NotNull Script script) {
        checkNotNull(script.getGitRepo(), "git repo cannot be null");
        checkNotNull(script.getGitCommitId(), "git commit id cannot be null");
        checkNotNull(script.getGitFolder(), "git folder  cannot be null");
        checkNotNull(script.getFilePath(), "git file path cannot be null");
        checkNotNull(script.getExecutionEnvironment(), "execution env cannot be null");
        return scriptActor.save(script);
    }

    @GET
    @Timed
    @ExceptionMetered
    @Path("/{id}/download")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response downloadScript(@PathParam("id") Long id) throws DSPSvcException {
        Response response = null;
        String zipFileLocation = scriptAPI.getScriptDirectoryZipFile(id);
        if (zipFileLocation != null) {
            File file = new File(zipFileLocation);
            Response.ResponseBuilder builder = Response.ok(file);
            builder.header("Content-Disposition", "attachment; filename=" + file.getName());
            response = builder.build();
            log.debug(String.format("Inside downloadScript ==> fileName: %s, fileSize: %s ",
                    file.getName(), FileUtils.byteCountToDisplaySize(file.length())));
        } else {
            log.error("Folder not avaiable in github for scriptEntity id : " + id);
            response = Response.status(404).entity("FILE NOT FOUND: ").type(MediaType.TEXT_PLAIN).build();
        }
        return response;
    }

    @GET
    @Path("/{id}")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Timed
    @UnitOfWork(readOnly = true)
    public ScriptMeta getScript(@PathParam("id") Long id) {
        return scriptAPI.getScriptMeta(id);
    }
}
