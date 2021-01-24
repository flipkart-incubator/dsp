package transformers;

import com.flipkart.dsp.mesos.entities.Job;
import com.flipkart.dsp.mesos.entities.JobGroup;
import com.flipkart.dsp.mesos.exceptions.TransformationException;
import com.flipkart.dsp.mesos.transformers.StringToJobGroupTransformer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class StringToJobGroupTransformerTest {

    StringToJobGroupTransformer stringToJobGroupTransformer = new StringToJobGroupTransformer();

    @Test
    public void transform() throws IOException, TransformationException{
        String json = new String(Files.readAllBytes(Paths.get(getClass().getClassLoader().getResource("payload.json").getPath())));
        JobGroup actual = stringToJobGroupTransformer.transform(json);
        List<String> uris = new ArrayList<>();
        uris.add("file:///tmp/haha");
        Job.Volume volume = new Job.Volume("/tmp/","/tmp/", Job.MountMode.RO);
        List<Job.Volume> volumeList = new ArrayList<>();
        volumeList.add(volume);
        Job job = new Job(1.0, 1024, "set -x;cd /tmp/haha;cat /tmp/script.sh", "0.0.0.0:5000/debian:jessie-slim", "DOCKER", "BRIDGE", null, uris, volumeList, 3, "id1", "name1", false);
        JobGroup expected = new JobGroup(new ArrayList<Job>() {
            {
                add(job);
            }
        });
        Assert.assertTrue(expected.equals(actual));
    }

    //You shall not pass
    @Test(expected = TransformationException.class)
    public void transformFail()throws IOException, TransformationException {
        String json = new String(Files.readAllBytes(Paths.get(getClass().getClassLoader().getResource("payloadFail.json").getPath())));
        stringToJobGroupTransformer.transform(json);
    }

    @Test
    public void transformList() throws IOException, TransformationException{
        String json = new String(Files.readAllBytes(Paths.get(getClass().getClassLoader().getResource("payloadList.json").getPath())));
        List<JobGroup> jobGroupList = stringToJobGroupTransformer.transformList(json);
        List<String> uris = new ArrayList<>();
        uris.add("file:///tmp/haha");
        Job.Volume volume = new Job.Volume("/tmp/","/tmp/", Job.MountMode.RO);
        List<Job.Volume> volumeList = new ArrayList<>();
        volumeList.add(volume);
        Job job = new Job(1.0, 1024, "set -x;cd /tmp/haha;cat /tmp/script.sh", "0.0.0.0:5000/debian:jessie-slim", "DOCKER", "BRIDGE", null, uris, volumeList, 3, "id1", "name1", true);
        JobGroup expected = new JobGroup(new ArrayList<Job>() {
            {
                add(job);
            }
        });
        List<JobGroup> expectedJobGroupList = new ArrayList<>();
        expectedJobGroupList.add(expected);
        Assert.assertTrue(expectedJobGroupList.equals(jobGroupList));
    }
}