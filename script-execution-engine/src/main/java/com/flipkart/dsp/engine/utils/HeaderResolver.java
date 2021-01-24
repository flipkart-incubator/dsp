package com.flipkart.dsp.engine.utils;

import com.flipkart.dsp.models.sg.Signal;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.models.variables.PandasDataFrame;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class HeaderResolver {

    private String getRHeader(List<String> headers) {
        String commaSeparated = StringUtils.join(headers, "\", \"");
        return StringUtils.wrap(commaSeparated, "\"");
    }

    private String getPythonHeader(List<String> headers) {
        String commaSeparated = StringUtils.join(headers, "\", \"");
        return StringUtils.wrap(commaSeparated, "\"");
    }

    public String getHeaders(LinkedHashSet<Signal> signalList, AbstractDataFrame abstractDataFrame) {
        List<String> headers = signalList.stream().map(s -> s.getName()).collect(Collectors.toList());
        if (abstractDataFrame instanceof PandasDataFrame) {
            return getPythonHeader(headers);
        } else {
            return getRHeader(headers);
        }
    }

    public void test() {
        System.out.println("HeaderResolver working....");
    }
}
