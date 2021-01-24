package com.flipkart.dsp.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

@Slf4j
public class ZipUtils {
    private static final int BUFFER_SIZE = 1024;
    private static void addDirToZipArchive(ZipOutputStream zos, File fileToZip, String parentDirectoryName) throws IOException {
        if (fileToZip == null || !fileToZip.exists()) {
            return;
        }

        String zipEntryName = fileToZip.getName();
        if (parentDirectoryName!=null && !parentDirectoryName.isEmpty()) {
            zipEntryName = parentDirectoryName + File.separator + fileToZip.getName();
        }

        if (fileToZip.isDirectory()) {
            log.debug("+" + zipEntryName);
            for (File file : fileToZip.listFiles()) {
                addDirToZipArchive(zos, file, zipEntryName);
            }
        } else {
            log.debug(" Zip entry Name : " + zipEntryName);
            byte[] buffer = new byte[BUFFER_SIZE];
            FileInputStream fis = new FileInputStream(fileToZip);
            zos.putNextEntry(new ZipEntry(zipEntryName));
            int length;
            while ((length = fis.read(buffer)) > 0) {
                zos.write(buffer, 0, length);
            }
            zos.closeEntry();
            fis.close();
        }
    }

    public static void zipDirectory(String sourceDir, String destZipFileLocation,boolean delSourceDir) throws IOException {
        if(!Files.isDirectory(Paths.get(sourceDir))) {
            throw new IOException("given source path is not a directory,  source directory path : " + sourceDir);
        }
        Files.createFile(Paths.get(destZipFileLocation));
        try (FileOutputStream fos = new FileOutputStream(destZipFileLocation);
             ZipOutputStream zos = new ZipOutputStream(fos);) {
            addDirToZipArchive(zos, new File(sourceDir), null);
            zos.flush();
            fos.flush();
        }
        if (delSourceDir) {
            FileUtils.deleteDirectory(new File(sourceDir));
        }
    }

    public static void unzipFileIntoDirectory(String sourceZipFileLocation, String destDir, boolean shouldDeleteZipFile) throws IOException {
        if(!Files.exists(Paths.get(sourceZipFileLocation))){
            throw new IOException("Zip File does not exist : " + sourceZipFileLocation);
        }
        if(Files.exists(Paths.get(destDir))) {
            FileUtils.cleanDirectory(new File(destDir));
        } else {
            Files.createDirectories(Paths.get(destDir));
        }
        unZip(sourceZipFileLocation,destDir,shouldDeleteZipFile);
    }

    private static void unZip(String zipFile, String destinationDir, boolean shouldDeleteZipFile) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];

        //get the zip file content
        try (ZipInputStream zis =
                     new ZipInputStream(new FileInputStream(zipFile));)
        {
            //get the zipped file list entry
            ZipEntry ze = zis.getNextEntry();

            while (ze != null) {

                String fileName = ze.getName();
                if (fileName.contains("__MACOSX") /*Ignore files created by OS while zipping*/) {
                    ze = zis.getNextEntry();
                } else {
                    File newFile = new File(destinationDir + File.separator + fileName);
                    log.debug("file unzipped : {}", newFile.getAbsoluteFile());
                    //create all non existing folders to avoid FileNotFoundException
                    new File(newFile.getParent()).mkdirs();
                    FileOutputStream fos = new FileOutputStream(newFile);
                    int len;
                    while ((len = zis.read(buffer)) > 0) {
                        fos.write(buffer, 0, len);
                    }

                    fos.close();
                    ze = zis.getNextEntry();
                }
            }
            zis.closeEntry();
        }

        if(shouldDeleteZipFile) {
            Files.delete(Paths.get(zipFile));
        }
    }
}
