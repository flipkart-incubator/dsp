package com.flipkart.manager;

import com.flipkart.exception.TestBedException;
import com.flipkart.utils.DSPConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Slf4j
public class TestBedManager {
    // This alias would be used in bucket so that we don't have to use Machine IPs.
    // Localhost can't be used as each container has a different localhost mapping

    /**
     * This method will create .env file will have all the details(placeholders) which docker compose will required
     */
    public void createEnvFileForDockerCompose(String sourceFilePath,
                                              String composeFileFolderPath,
                                              Map<String, String> placeholderValueMap) throws TestBedException {
        Path envFileDestination = Paths.get(composeFileFolderPath, DSPConstants.ENV_FILE_NAME);
        try {
            File file = new File(sourceFilePath);
            final String[] contents = {FileUtils.readFileToString(file, StandardCharsets.UTF_8.name())};

            placeholderValueMap.entrySet().stream().forEach((k) -> {
                Pattern regex = Pattern.compile(k.getKey());
                contents[0] = regex.matcher(contents[0]).replaceAll(k.getValue());
            });

            file = new File(envFileDestination.toUri());
            FileUtils.writeStringToFile(file, contents[0]);
            log.info("Finished writing to .env file" + contents[0]);

        } catch (IOException e) {
           throw new TestBedException("Unable to create .env file required by compose", e);
        }
    }

    // This will populate the host file and also set an environment variable
    public void executeScript(String initScriptPath) throws TestBedException {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.command("sudo","sh",initScriptPath);
            Process process = processBuilder.start();
            process.waitFor();
        } catch (IOException | InterruptedException e) {
            throw new TestBedException("Error in Running init script", e);
        }
    }


    public void initiateDockerCompose(String composeFileFolderPath) throws TestBedException {
        List<String> commands = new ArrayList<>();
        commands.add("bash");
        commands.add("-c");
        commands.add("docker-compose up");
        Process dockerComposeCommand;
        try {
            String hostIP = InetAddress.getLocalHost().getHostAddress();
            System.setProperty("HOST_MACHINE_IP", hostIP);

        } catch (UnknownHostException e) {
            throw new TestBedException("Unable to set HOST_MACHINE_IP system variable", e);
        }
        ProcessBuilder builder = new ProcessBuilder();
        Map<String, String> env = builder.environment();
        try {
            env.put("HOST_MACHINE_IP", InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException e) {
            throw new TestBedException("Unable to set HOST_MACHINE_IP system variable", e);
        }
        builder.command(commands);
        builder.directory(new File(composeFileFolderPath));
        String path = System.getenv("PATH");
        builder.environment().put("PATH","/usr/bin:"+path);
        builder.redirectErrorStream(true);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);

        try {
            dockerComposeCommand = builder.start();
            log.info("Waiting for Docker to Start");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(dockerComposeCommand.getInputStream()))) {
                String line = reader.readLine();
                while (line != null) {
                    log.info(line);
                    if(line.contains("Started ipp-dsp-service")) {
                        break;
                    }
                    line = reader.readLine();
                }
            }
        } catch (IOException e) {
            throw new TestBedException(e.getMessage());
        }
    }

    public boolean distoryEnv(String composeFilePath) {
        List<String> commands = new ArrayList<>();
        commands.add("bash");
        commands.add("-c");
        commands.add("docker-compose down");
        Process dockerComposeCommand;

        ProcessBuilder builder = new ProcessBuilder();

        builder.command(commands);
        builder.directory(new File(composeFilePath));

        String path = System.getenv("PATH");
        builder.environment().put("PATH","/usr/bin:"+path);
        builder.redirectErrorStream(true);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);

        try {
            dockerComposeCommand = builder.start();
            log.info("Waiting for Environment to be destroyed");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(dockerComposeCommand.getInputStream()))) {
                String line = reader.readLine();
                while (line != null) {
                    log.info(line);
                    line = reader.readLine();
                }
            }
        } catch (IOException e) {
            log.error("Error in destroying environment " + e.getMessage());
            return false;
        }
        return true;
    }

    public void pruneExistingDockerNetwork() {
        log.info("Pruning Existing Network");
        List<String> networkPruneCmdList = new ArrayList<>();
        networkPruneCmdList.add("docker container stop $(docker ps -a -q)");
        networkPruneCmdList.add("docker container rm $(docker ps -a -q)");
        networkPruneCmdList.add("docker network prune --force");
        for(String cmd : networkPruneCmdList) {
            List<String> commands = new ArrayList<>();
            commands.add("bash");
            commands.add("-c");
            commands.add(cmd);
            Process dockerComposeCommand;

            ProcessBuilder builder = new ProcessBuilder();
            builder.command(commands);
            try {
                dockerComposeCommand = builder.start();
                log.info("Command getting executed : " + cmd);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(dockerComposeCommand.getInputStream()))) {
                    String line = reader.readLine();
                    while (line != null) {
                        log.info(line);
                        line = reader.readLine();
                    }
                }
            } catch (IOException e) {
                log.error("Error in destroying environment " + e.getMessage());
            }
        }
    }

    public void addExtraResourcesForSGExecution() {
        log.info("Giving additional resources to mesos slave container fro SG Execution");
         String command = "docker update --cpuset-cpus 8 mesos_slavee_compose"; // "mesos_slavee_compose" container name
        List<String> commands = new ArrayList<>();
        commands.add("bash");
        commands.add("-c");
        commands.add(command);
        ProcessBuilder builder = new ProcessBuilder();
        builder.command(commands);
        try {
            builder.start();
            log.info("Command Execute Successfully : " + command);
        } catch (IOException e) {
            log.error("Error while updating resources " + e.getMessage());
        }
    }
}
