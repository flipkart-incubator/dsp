package com.flipkart.utils;

import com.flipkart.exception.FileOperationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
public class FileOperationsHelper {

    public String writeToFile(Path localFilePath, String content) throws FileOperationException {
        File file = new File(localFilePath.toUri());
        try {
            IOUtils.write(content, new FileOutputStream(file), "UTF-8");
        } catch (IOException e) {
            throw new FileOperationException("Unable to write to file " + e.getMessage());
        }
        return localFilePath.toString();
    }

    public void createDirectoryIfNotExist(String folderPath) throws FileOperationException  {
        Path path = Paths.get(folderPath, "/");
        if(Files.notExists(path)) {
            try {
                log.debug("Directory created at path " + path.toString());
                Files.createDirectories(path);
            } catch (IOException e) {
                throw new FileOperationException("Unable to create local Directory " + e.getMessage());
            }
        }
    }

    public String getFileContent(String resourcePath) throws FileOperationException {
        InputStream inputStream = this.getClass().getResourceAsStream(resourcePath);
        try {
            String content = IOUtils.toString(inputStream, "UTF-8");
            log.debug("Reading content of file  " + resourcePath);
            return content;
        } catch (IOException e) {
            throw new FileOperationException("Unable to download the Sample Yaml file " + e.getMessage());
        }
    }

    public String getFileFromResource(String fileRelativePath, String folder) throws FileOperationException  {
        createDirectoryIfNotExist(folder);
        String content = getFileContent(fileRelativePath);
        String fileName = fileRelativePath.substring(fileRelativePath.lastIndexOf("/")+1);
        Path localFilePath = Paths.get(folder, fileName);
        return writeToFile(localFilePath, content);
    }
}
