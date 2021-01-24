package schedulers;

import com.flipkart.dsp.mesos.constants.ResourceConstants;
import com.flipkart.dsp.mesos.entities.Job;
import com.flipkart.dsp.mesos.exceptions.SchedulerException;
import com.flipkart.dsp.mesos.schedulers.FirstFitSchedulingAlgorithm;
import org.apache.mesos.Protos;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FirstFitSchedulingAlgorithmTest {

    FirstFitSchedulingAlgorithm firstFitSchedulingAlgorithm = new FirstFitSchedulingAlgorithm();

    @Test
    public void schedule() {
        Protos.Offer offer = Protos.Offer.newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue("id1").build())
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("frameworkid1").build())
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("slaveid1").build())
                .setHostname("hostHaha")
                .addResources(Protos.Resource.newBuilder()
                        .setName(ResourceConstants.CPUS)
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder()
                                .setValue(1)
                                .build())
                        .build())
                .addResources(Protos.Resource.newBuilder()
                        .setName(ResourceConstants.MEMORY)
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder()
                                .setValue(1024)
                                .build())
                        .build())
                .build();

        Job job = new Job(1.0, 1024, "set -x;cd /tmp/haha;cat /tmp/script.sh", "0.0.0.0:5000/debian:jessie-slim", "DOCKER", "BRIDGE", null, null, null, 3, "id1", "name1", false);

        Map<Protos.Offer, List<Job>> offerToJobListMap = firstFitSchedulingAlgorithm.schedule(new ArrayList<Protos.Offer>(){{
            add(offer);
        }}, new ArrayList<Job>() {
            {
                add(job);
            }
        });

        Assert.assertTrue(!offerToJobListMap.isEmpty());
        Assert.assertTrue(offerToJobListMap.size() == 1);
        Assert.assertTrue(offerToJobListMap.get(offer).equals(new ArrayList<Job>(){
            {
                add(job);
            }
        }));
    }

    @Test(expected = SchedulerException.class)
    public void scheduleFail() {
        firstFitSchedulingAlgorithm.schedule(null,null);
    }

    @Test
    public void schedule2() {
        Protos.Offer offer1 = Protos.Offer.newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue("id1").build())
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("frameworkid1").build())
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("slaveid1").build())
                .setHostname("hostHaha")
                .addResources(Protos.Resource.newBuilder()
                        .setName(ResourceConstants.CPUS)
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder()
                                .setValue(5)
                                .build())
                        .build())
                .addResources(Protos.Resource.newBuilder()
                        .setName(ResourceConstants.MEMORY)
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder()
                                .setValue(5024)
                                .build())
                        .build())
                .build();

        Protos.Offer offer2 = Protos.Offer.newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue("id2").build())
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("frameworkid1").build())
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("slaveid1").build())
                .setHostname("hostHaha")
                .addResources(Protos.Resource.newBuilder()
                        .setName(ResourceConstants.CPUS)
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder()
                                .setValue(10)
                                .build())
                        .build())
                .addResources(Protos.Resource.newBuilder()
                        .setName(ResourceConstants.MEMORY)
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder()
                                .setValue(5024)
                                .build())
                        .build())
                .build();

        Job job1 = new Job(2.0, 1024, "set -x;cd /tmp/haha;cat /tmp/script.sh", "0.0.0.0:5000/debian:jessie-slim", "DOCKER", "BRIDGE", null, null, null, 3, "id1", "name1", false);
        Job job2 = new Job(3.0, 1024, "set -x;cd /tmp/haha;cat /tmp/script.sh", "0.0.0.0:5000/debian:jessie-slim", "DOCKER", "BRIDGE", null, null, null, 3, "id1", "name1", false);
        Job job3 = new Job(5.0, 1024, "set -x;cd /tmp/haha;cat /tmp/script.sh", "0.0.0.0:5000/debian:jessie-slim", "DOCKER", "BRIDGE", null, null, null, 3, "id1", "name1", false);

        Map<Protos.Offer, List<Job>> expectedOfferToJobListMap = new HashMap<>();
        expectedOfferToJobListMap.put(offer1,new ArrayList<Job>() {
            {
                add(job1);
                add(job2);
            }
        });
        expectedOfferToJobListMap.put(offer2,new ArrayList<Job>() {
            {
                add(job3);
            }
        });
        Map<Protos.Offer, List<Job>> actualOfferToJobListMap = firstFitSchedulingAlgorithm.schedule(new ArrayList<Protos.Offer>(){
            {
                add(offer1);
                add(offer2);
            }
        },new ArrayList<Job>() {
            {
                add(job1);
                add(job2);
                add(job3);
            }
        });
        Assert.assertEquals(expectedOfferToJobListMap,actualOfferToJobListMap);
    }
}
