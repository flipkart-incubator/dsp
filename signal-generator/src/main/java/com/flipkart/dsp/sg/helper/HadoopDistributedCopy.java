package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.qe.entity.HiveConfigParam;
import com.flipkart.dsp.qe.utils.RetryWaitLogic;
import com.flipkart.dsp.sg.exceptions.DistCpException;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static com.flipkart.dsp.utils.Constants.slash;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class HadoopDistributedCopy {
    private final HiveConfigParam hiveConfigParam;

    public void run(Configuration configuration, String source, String destination, boolean overwrite,
                    Integer maxMappers, String destBasePath) {
        List<String> arguments = new ArrayList<>();
        if (overwrite) arguments.add("-overwrite");
        arguments.add("-pb"); //Preserve block size while copying distcp
        arguments.add("-m " + maxMappers);

        destination = destination.startsWith(slash) ? destBasePath + destination : destBasePath + slash + destination;

        List<Path> sources = new LinkedList<>();
        Path hadoopDestination = new Path(destination);
        sources.add(new Path(source + slash));
        arguments.add(source + slash);
        arguments.add(destination);
        performDistcp(configuration, source, destination, arguments, sources, hadoopDestination);
    }

    private void performDistcp(Configuration configuration, String source, String destination, List<String> arguments, List<Path> sources, Path hadoopDestination) {
        int retryGapInMillis = hiveConfigParam.getRetryGapInMillis();
        for (int i = 0; i < retryGapInMillis; i++) {
            try {
                DistCpOptions distCpOptions = new DistCpOptions(sources, hadoopDestination);
                DistCp distCp = new DistCp(configuration, distCpOptions);
                int exitCode = ToolRunner.run(distCp, arguments.toArray(new String[0]));
                if (exitCode != 0) {
                    throw new DistCpException("Distributed Copy action failed for: source:" + source + " , destination: " + destination);
                }
                return;
            } catch (Exception ex) {
                if (i == hiveConfigParam.getMaxRetries() - 1) {
                    throw new DistCpException("Distributed Copy action failed for: source:" + source + " , destination: " + destination);
                }
                retryGapInMillis = RetryWaitLogic.backOffAndWait(retryGapInMillis);
            }
        }
    }

}
