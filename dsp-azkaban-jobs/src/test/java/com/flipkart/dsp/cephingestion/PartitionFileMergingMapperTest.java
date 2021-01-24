//package com.flipkart.dsp.ceph_ingestion;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.security.UserGroupInformation;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//import org.powermock.api.mockito.PowerMockito;
//import org.powermock.core.classloader.annotations.PowerMockIgnore;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.util.stream.Stream;
//
//import static com.flipkart.dsp.utils.Constants.FIELD_DELIMITER;
//import static com.flipkart.dsp.utils.Constants.comma;
//import static org.junit.Assert.assertTrue;
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.Mockito.*;
//import static org.powermock.api.mockito.PowerMockito.verifyNew;
//
///**
// * +
// */
//@RunWith(PowerMockRunner.class)
//@PowerMockIgnore({"javax.management_", "javax.xml.", "org.w3c.", "org.apache.apache._", "com.sun.*"})
//@PrepareForTest({PartitionFileMergingMapper.class,  InputStreamReader.class, BufferedReader.class, ObjectMapper.class, UserGroupInformation.class})
//public class PartitionFileMergingMapperTest {
//
//    private FileSystem fileSystem;
//    @Mock private Mapper.Context context;
//    @Mock private ObjectMapper objectMapper;
//    private Configuration configuration;
//    @Mock private BufferedReader bufferedReader;
//    @Mock private InputStreamReader inputStreamReader;
//    @Mock private FSDataInputStream fsDataInputStream;
//    private PartitionFileMergingMapper partitionFileMergingMapper;
//
//    @Before
//    public void setUp() throws Exception {
//        configuration = new Configuration();
//        configuration.set(FIELD_DELIMITER, comma);
//        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("fk-ip-data-service");
//
//        fileSystem = mock(FileSystem.class);
////        PowerMockito.mockStatic(FileSystem.class);
//        PowerMockito.mockStatic(UserGroupInformation.class);
//        PowerMockito.mockStatic(ObjectMapper.class);
//        PowerMockito.mockStatic(BufferedReader.class);
//        PowerMockito.mockStatic(InputStreamReader.class);
//        MockitoAnnotations.initMocks(this);
//        this.partitionFileMergingMapper = spy(new PartitionFileMergingMapper());
//
////        PowerMockito.suppress(PowerMockito.constructor(FileSystem.class));
//        Stream<String> stringStream = Stream.of("a,b");
//        when(bufferedReader.lines()).thenReturn(stringStream);
//        when(context.getConfiguration()).thenReturn(configuration);
////        PowerMockito.when(FileSystem.get(configuration)).thenReturn(fileSystem);
//        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(ugi);
//        when(fileSystem.open(any())).thenReturn(fsDataInputStream);
//        PowerMockito.whenNew(ObjectMapper.class).withAnyArguments().thenReturn(objectMapper);
//        PowerMockito.whenNew(BufferedReader.class).withAnyArguments().thenReturn(bufferedReader);
//        when(objectMapper.readValue("fileSystem", FileSystem.class)).thenReturn(fileSystem);
//        PowerMockito.whenNew(InputStreamReader.class).withAnyArguments().thenReturn(inputStreamReader);
//    }
//
//    @Test
//    public void testMapSuccess() throws Exception {
//        doNothing().when(context).write(any(), any());
//
//        partitionFileMergingMapper.setup(context);
//        partitionFileMergingMapper.map(new LongWritable(1), new Text("FTP_TEST_WF/ceph_output_dataframe/testingCeph/refresh_id=72/analytic_vertical=ABC/a.csv"), context);
//        verify(context, times(2)).getConfiguration();
//        verify(configuration, times(1)).get(FIELD_DELIMITER);
//        verify(configuration, times(1)).get("fileSystem");
//        verify(objectMapper, times(1)).readValue("fileSystem", FileSystem.class);
//        verify(fileSystem, times(1)).open(any());
//        verify(context, times(1)).write(any(), any());
//        verifyNew(ObjectMapper.class).withNoArguments();
//        verifyNew(BufferedReader.class).withArguments(inputStreamReader);
//        verifyNew(InputStreamReader.class).withArguments(fsDataInputStream);
//    }
//
//    @Test
//    public void testMapFailure() throws Exception {
//        boolean isException = false;
//        doThrow(new IOException()).when(context).write(any(), any());
//
//        partitionFileMergingMapper.setup(context);
//        try {
//            partitionFileMergingMapper.map(new LongWritable(1), new Text("FTP_TEST_WF/ceph_output_dataframe/testingCeph/refresh_id=72/analytic_vertical=ABC/a.csv"), context);
//        } catch (Exception e) {
//            isException = true;
//            assertTrue(e.getMessage().contains("Error while running mapper while merging file for Ceph Upload"));
//        }
//
//        assertTrue(isException);
//        verify(context, times(2)).getConfiguration();
//        verify(configuration, times(1)).get(FIELD_DELIMITER);
//        verify(configuration, times(1)).get("fileSystem");
//        verify(objectMapper, times(1)).readValue("fileSystem", FileSystem.class);
//        verify(fileSystem, times(1)).open(any());
//        verify(context, times(1)).write(any(), any());
//        verifyNew(ObjectMapper.class).withNoArguments();
//        verifyNew(BufferedReader.class).withArguments(inputStreamReader);
//        verifyNew(InputStreamReader.class).withArguments(fsDataInputStream);
//    }
//}
