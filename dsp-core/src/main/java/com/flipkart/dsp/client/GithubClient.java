package com.flipkart.dsp.client;

import com.flipkart.dsp.config.GithubConfig;
import com.flipkart.dsp.utils.ZipUtils;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.kohsuke.github.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Singleton
public class GithubClient {

    private final GithubConfig config;
    private final GitHub gitHub;

    @Inject
    public GithubClient(GithubConfig config) throws IOException {
        this.config = config;
        this.gitHub = GitHub.connectToEnterpriseWithOAuth(config.getApiUrl(), config.getLogin(), config.getToken());
    }

    public void checkValidity(String gitRepo, String commitId) throws IOException {
        GHRepository repository = gitHub.getRepository(gitRepo);
        repository.getCommit(commitId);
    }

    public String getLatestCommit(String gitRepo, String commitSHAOrBranch) throws IOException {
        GHRepository repository = gitHub.getRepository(gitRepo);
        Map<String, GHBranch> branches = repository.getBranches();
        if (branches.containsKey(commitSHAOrBranch)) {
            log.debug("Latest commit for branch {} is {}", commitSHAOrBranch, branches.get(commitSHAOrBranch).getSHA1());
            return branches.get(commitSHAOrBranch).getSHA1();
        } else {
            log.debug("Not a branch but commit id {}", commitSHAOrBranch);
            return commitSHAOrBranch;
        }
    }
    /**
     *
     * @param gitRepo
     * @param commitSHA eg : "e783c20e931efd9df69f0da36c45e61fdeca32c7"
     * @param directoryPath eg : "production/DEMAND_PLANNING/v1"
     * @param localDirectoryPath eg : "/tmp/20180211"
     * @param fetchRecursive eg : "true" flag which indicates whether to recursively fetch all directories
     * @throws IOException
     */
    public void fetchDirectoryContent(String gitRepo, String commitSHA, String directoryPath, String localDirectoryPath, boolean fetchRecursive) throws IOException {
        validateRepoName(gitRepo);
        GHRepository repository = gitHub.getRepository(gitRepo);
        validateCommit(repository, commitSHA);
        downloadDirectoryContent(repository,commitSHA,directoryPath,localDirectoryPath,fetchRecursive,config.getAllowedFileExtensions());
    }

    public boolean isValidFileInGithubForGivenCommitId(String gitRepo, String gitFolder, String gitFilePath, String commitSHA) throws IOException {
        String completeFilePath;
        if (gitFolder.isEmpty() || gitFolder.equals("/")) {
            completeFilePath = gitFilePath;
        } else {
            completeFilePath = String.format("%s/%s", gitFolder, gitFilePath);
        }
        GHRepository repository = gitHub.getRepository(gitRepo);
        return config.getAllowedFileExtensions().contains(getFileExtensionInUpperCase(completeFilePath)) && repository.getFileContent(completeFilePath, commitSHA).isFile();
    }

    private void downloadDirectoryContent(GHRepository repository, String commitSHA, String remoteDirectoryPath, String localDirectoryPath, boolean fetchRecursive, Set<String> allowedFileExtensions) throws IOException {
        List<GHContent> contentList = repository.getDirectoryContent(remoteDirectoryPath, commitSHA);
        if (Files.exists(Paths.get(localDirectoryPath))) {
            FileUtils.deleteDirectory(new File(localDirectoryPath));
        }
        Files.createDirectories(Paths.get(localDirectoryPath));
        for (GHContent ghContent : contentList) {
            String currentLocalPath = localDirectoryPath + File.separator + ghContent.getName();
            if (ghContent.isDirectory() && fetchRecursive) {
                downloadDirectoryContent(repository, commitSHA, ghContent.getPath(), currentLocalPath, fetchRecursive, allowedFileExtensions);
            } else {
                if (ghContent.getPath().contains(".") && allowedFileExtensions.contains(getFileExtensionInUpperCase(ghContent.getPath()))) {
                    downloadFileContent(ghContent, currentLocalPath);
                }
            }
        }
    }
    private String getFileExtensionInUpperCase(String filePath) {
        return filePath.substring(filePath.lastIndexOf(".") + 1).toUpperCase();
    }

    private void downloadFileContent(GHContent ghContent, String localFilePath) throws IOException {
            Files.deleteIfExists(Paths.get(localFilePath));
            Files.createFile(Paths.get(localFilePath));
            InputStream inputStream = ghContent.read();
            OutputStream outputStream = new FileOutputStream(localFilePath);
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(outputStream);
    }

    private void validateRepoName(String repoName) throws IOException {
        if (!repoName.matches(".*/.*")) {
            throw new IOException("Repo Name should be of the format <Organization>/<Repo Name>");
        }
    }

    private void validateCommit(GHRepository repository, String commitSHA) throws IOException {
        try {
            repository.getCommit(commitSHA);
        } catch (FileNotFoundException e) {
            throw new IOException("Following commitId is not present : " + commitSHA);
        }
    }

}
