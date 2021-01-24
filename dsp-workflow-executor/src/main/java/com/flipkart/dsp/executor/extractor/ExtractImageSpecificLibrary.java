package com.flipkart.dsp.executor.extractor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.executor.exception.ExtractImageSpecificLibraryException;
import com.flipkart.dsp.models.ExecutionEnvironmentDetails;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ExtractImageSpecificLibrary {
    private final ObjectMapper objectMapper;

    public Map<String, ExecutionEnvironmentDetails.NativeLibraryDetails> getImageSpecificLibrary(Map<String, ExecutionEnvironmentDetails.NativeLibraryDetails> nativeLibrary) throws ExtractImageSpecificLibraryException {

        List<String> librariesToBeExcluded = getExcludedLibraryList();
        Map<String, ExecutionEnvironmentDetails.NativeLibraryDetails> imageSpecificLibraryMap =
                new HashMap<>();
        for (Map.Entry<String, ExecutionEnvironmentDetails.NativeLibraryDetails> entry : nativeLibrary.entrySet()) {
            if (!librariesToBeExcluded.contains(entry.getKey())) {
                imageSpecificLibraryMap.put(entry.getKey(), entry.getValue());
            }
        }
        return imageSpecificLibraryMap;
    }

    private List<String> getExcludedLibraryList() throws ExtractImageSpecificLibraryException {
        try {
            String excludedLibraryList = readExcludeLibrariesFromFile();
            return objectMapper.readValue(excludedLibraryList, List.class);
        } catch (Exception e) {
            throw new ExtractImageSpecificLibraryException("Issue in reading file containing default Libraries " + e.getMessage());
        }
    }

     private String readExcludeLibrariesFromFile() throws ExtractImageSpecificLibraryException {
        String val = "";

        try {
            InputStream i = ClassLoader.getSystemResourceAsStream("os_native_libraries.json");
            BufferedReader r = new BufferedReader(new InputStreamReader(i));
            String l;
            while((l = r.readLine()) != null) {
                val = val + l;
            }
            i.close();
        } catch(Exception e) {
            throw new ExtractImageSpecificLibraryException("Unable to read Excluded Library Details " + e.getMessage());
        }
        return val;
    }
}
