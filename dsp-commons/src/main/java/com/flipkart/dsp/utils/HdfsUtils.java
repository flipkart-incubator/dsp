package com.flipkart.dsp.utils;


import com.flipkart.dsp.exceptions.HDFSUtilsException;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


/**
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class HdfsUtils {
    private final FileSystem fs;

    public long getFolderSize(String path) throws IOException {
        Path path1 = new Path(path);
        ContentSummary contentSummary = fs.getContentSummary(path1);
        long length = contentSummary.getLength();
        long spaceConsumed = contentSummary.getSpaceConsumed();
//        log.info(contentSummary.toString(false, true));
        log.debug("Path: {} , File/Folder Size : {}, Disk Space Consumed: {}",
                path,
                StringUtils.TraditionalBinaryPrefix.long2String(length, "", 1),
                StringUtils.TraditionalBinaryPrefix.long2String(spaceConsumed, "", 1));
        return length;
    }

    public void cleanFilesUnderDirectory(Path path) throws HDFSUtilsException {
        try {
            fs.delete(path, true);
            fs.mkdirs(path);
        } catch (IOException e) {
            throw new HDFSUtilsException(String.format("Unable to delete directory  %s, Message: %s", path, e.getMessage()));
        }
    }

    public FileStatus[] getFilesUnderDirectory(Path path) throws HDFSUtilsException {
        try {
            return fs.globStatus(path);
        } catch (IOException e) {
            throw new HDFSUtilsException("Exception received while trying to get the files under the directory : " + path, e);
        }
    }

    public List<String> getAllFileRelativePath(String filePath) throws HDFSUtilsException {
        List<String> fileList = getAllFilePath(new Path (filePath));
        List<String> relativeFileList = new ArrayList<>();
        for (String file : fileList) {
            relativeFileList.add(file.split(filePath)[1]);
        }
        return relativeFileList;
    }

    public List<String> getAllFilePath(Path filePath) throws HDFSUtilsException {
        List<String> fileList = new ArrayList<>();
        try {
            FileStatus[] fileStatus = fs.listStatus(filePath);
            for (FileStatus fileStat : fileStatus) {
                if (fileStat.isDirectory()) {
                    fileList.addAll(getAllFilePath(fileStat.getPath()));
                } else {
                    String fileName = fileStat.getPath().toString();
                    if (!fileName.substring(fileName.lastIndexOf("/") + 1).startsWith(".")) {
                        fileList.add(fileName);
                    }
                }
            }
            return fileList;
        } catch (IOException e) {
            throw new HDFSUtilsException("Exception received while trying to get the files under the directory : " + filePath, e);
        }
    }

    public boolean createHDFSDirIfNotExists(Path hdfsDir) throws IOException{
        if(!fs.exists(hdfsDir)) {
            fs.mkdirs(hdfsDir);
            fs.setPermission(hdfsDir, new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
            log.info("Made hdfs directory " + hdfsDir);
            return true;
        } else {
            return false;
        }
    }

    public long getFileSize(String hdfsFilePath) throws IOException{
        Path path = new Path(hdfsFilePath);
        long len = fs.getFileStatus(path).getLen();
        return len;
    }

    /**
     * The method checks weather the given hdfs directory path exist or not.
     * @param hdfsDirectoryPath
     * @return
     * @throws IOException
     */
    public boolean hdfsPathExist(String hdfsDirectoryPath) throws IOException {
        Path path = new Path(hdfsDirectoryPath);
        return fs.exists(path);
    }


    /**
     * The method checks weather the given hdfs path is of directory or file.
     * @param hdfsDirectoryPath
     * @return
     * @throws IOException
     */
    public boolean isDirectory(String hdfsDirectoryPath) throws IOException{
        return fs.isDirectory(new Path(hdfsDirectoryPath));
    }


    /**
     *  The method checks weather the given hdfs directory is empty or not.
     * @param hdfsDirectoryPath
     * @return
     * @throws IOException
     */
    public boolean isEmptyHdfsDirectory(String hdfsDirectoryPath) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsDirectoryPath));
        return (fileStatuses.length == 0) ? true : false;
    }

    /**
     * The method copies the file from given hdfs location to given local directory
     * @param hdfsDirectoryPath
     * @param localDirectoryPath
     * @throws Exception
     */
    public void copyFromHdfsToLocal(String hdfsDirectoryPath, String localDirectoryPath) throws IOException {
        log.info("Copying file from hdfs location: " + hdfsDirectoryPath + " to local path: " + localDirectoryPath);
        this.createLocalDirectoriesIfNotExists(localDirectoryPath);
        fs.copyToLocalFile(new Path(hdfsDirectoryPath), new Path(localDirectoryPath));
    }

    /**
     * The method checks weather the given local directory path exists or not. If does not exist creates the given directory.
     * @param localDirectoryPath
     * @throws IOException
     */
    public Boolean createLocalDirectoriesIfNotExists(String localDirectoryPath) throws IOException {
        java.nio.file.Path path = Paths.get(localDirectoryPath);
        Boolean exists = Files.exists(path, new LinkOption[]{ LinkOption.NOFOLLOW_LINKS});
        log.info("Does LocalDirectoryPath: " + localDirectoryPath + " exists: " + exists);
        if(!exists){
            Files.createDirectories(path);
            return false;
        }
        return true;
    }

    public Boolean copyFromLocalToHdfs(String sourceHdfsFilePath, String destnationHdfsDirectoryPath) throws IOException, HDFSUtilsException {
        log.info("Moving file from Local Filesystem: " + sourceHdfsFilePath + " to HDFS Filesystem: " + destnationHdfsDirectoryPath);
        Path destination = new Path(destnationHdfsDirectoryPath);
        if (!fs.exists(destination)) {
            createHDFSDirIfNotExists(destination);
        } else {
            cleanFilesUnderDirectory(new Path(destnationHdfsDirectoryPath));
        }
        fs.copyFromLocalFile(new Path(sourceHdfsFilePath), destination);
        return true;
    }

    public  String loadFromHDFS(String fileLocation, String lineSeparator) {
        StringBuilder sb = new StringBuilder();
        try{
            Path path = new Path(fileLocation);
            readFromPath(sb, path, lineSeparator);
        } catch (Exception e){
            log.error("Unable to load file from hdfs", e);
            throw new RuntimeException("Unable to load file from HDFS:" + fileLocation, e);
        }
        return sb.toString();
    }

    private void readFromPath(StringBuilder sb, Path path, String lineSeparator) throws IOException {
        InputStreamReader is = new InputStreamReader(fs.open(path));
        BufferedReader br = new BufferedReader(is);
        String line = br.readLine();
        while (line != null){
            sb.append(line);
            sb.append(lineSeparator);
            line = br.readLine();
        }
        is.close();
        br.close();
    }

    public List<String> getFileNamesUnderDirectory(Path path) throws IOException {
        List<String> fileList = new ArrayList<>();
        FileStatus[] files =  fs.globStatus(path);
        for (FileStatus file: files) {
            if(!file.isDirectory()) fileList.add(file.getPath().toString());
        }
        return fileList;
    }

    public List<String> getAllFilesUnderDirectory(Path path) throws IOException {
        List<String> fileList = new ArrayList<>();
        FileStatus[] files =  fs.listStatus(path);
        for (FileStatus file: files) {
            if(!file.isDirectory()) fileList.add(file.getPath().toString());
            else
                fileList.addAll(getAllFilesUnderDirectory(file.getPath()));
        }
        return fileList;
    }

    public void writeToFile(String content, String outputPath) throws IOException {
        Path path = new Path(outputPath);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        OutputStream os = fs.create(path);

        InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
        IOUtils.copyBytes(is, os, new Configuration());


        //Close the streams
        is.close();
        os.close();
    }

    public FileStatus[] getListStatus(String hdfsPath) throws IOException {
        return fs.listStatus(new Path(hdfsPath));
    }

    public void deleteIfExist(String dir) throws IOException, HDFSUtilsException {
        if (hdfsPathExist(dir))
            fs.delete(new Path(dir), true);

    }

    public void concatFiles(Path sourceFile, Path[] fileToBeConcatenated) throws IOException {
        fs.concat(sourceFile, fileToBeConcatenated);
    }

    public boolean rename(String currentName, String updatedName) throws IOException {
        return fs.rename(new Path(currentName), new Path(updatedName));
    }

    public InetSocketAddress getActiveNameNode(String clusterAddress) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", clusterAddress);
        FileSystem fileSystem = FileSystem.get(conf);
        return HAUtil.getAddressOfActive(fileSystem);
    }
}
