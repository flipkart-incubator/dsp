package com.flipkart.dsp.health_check;

import com.flipkart.dsp.config.GithubConfig;
import com.flipkart.dsp.exceptions.HealthCheckException;
import com.flipkart.dsp.models.ExternalClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;

import javax.inject.Inject;

import static com.flipkart.dsp.utils.Constants.HEALTH_CHECK_ERROR;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class GithubHealthCheck implements HealthCheck {

    public void check() throws HealthCheckException {
        String repoName = "dsp-sandbox-trial";
        String dspRepo = "Flipkart/" + repoName;

        try {
            GithubConfig githubConfig = new GithubConfig("https://github.fkinternal.com/api/v3",githubConfig.getLogin(),githubConfig.getLogin(),"R,py", null,"/tmp/script-cache");
            GitHub gitHub = GitHub.connectToEnterpriseWithOAuth(githubConfig.getApiUrl(), githubConfig.getLogin(), githubConfig.getToken());
            GHRepository repository = gitHub.getRepository(dspRepo);

            if (repository.getName().equalsIgnoreCase(repoName))
                log.info(String.format("Successfully polled the git hub Repo %s, Github Health Check Successful", dspRepo));
            else {
                log.info(String.format("Unable to poll repo %s", dspRepo));
                throw new HealthCheckException(String.format("Health Check failed for %s", ExternalClient.GITHUB.toString()));
            }

        } catch (Exception e) {
            throw new HealthCheckException(String.format(HEALTH_CHECK_ERROR, ExternalClient.GITHUB.toString()));
        }
    }
}
