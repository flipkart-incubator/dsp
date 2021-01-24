package com.flipkart.dsp.qe.entity;

import com.flipkart.dsp.qe.clients.HiveClient;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
@Ignore
public class MetaStoreClientVsHiveClientTest {
    private MetaStoreClient metaStoreClient;
    private HiveMetaStoreClient hiveMetaStoreClient;
    private HiveClient hiveClient;
    private static final String tableName="gouri.India";

    @Test
    public void getConnection() throws MetaException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://0.0.0.0:9083");
        hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        metaStoreClient = new MetaStoreClient(hiveMetaStoreClient);
        final String url = "jdbc:hive2://prod-hadoop-hadoopcluster2-jn-0001:2181,prod-hadoop-hadoopcluster2-jn-0002:2181,prod-hadoop-hadoopcluster2-jn-0003:2181,prod-hadoop-hadoopcluster2-jn-0004:2181,prod-hadoop-hadoopcluster2-jn-0005:2181/;transportMode=http;httpPath=cliservice;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hive_hadoopcluster";
        final String user = "fk-ip-data-service";
        HiveConfigParam hiveConfigParam = new HiveConfigParam(url, user, "", 40, 100, 3, 0);
        hiveClient = new HiveClient(hiveConfigParam);
    }

    @Test
    public void getTableLocationTest() throws HiveClientException, TException, TableNotFoundException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://0.0.0.0:9083");
        hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        metaStoreClient = new MetaStoreClient(hiveMetaStoreClient);
        final String url = "jdbc:hive2://prod-hadoop-hadoopcluster2-jn-0001:2181,prod-hadoop-hadoopcluster2-jn-0002:2181,prod-hadoop-hadoopcluster2-jn-0003:2181,prod-hadoop-hadoopcluster2-jn-0004:2181,prod-hadoop-hadoopcluster2-jn-0005:2181/;transportMode=http;httpPath=cliservice;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hive_hadoopcluster";
        final String user = "fk-ip-data-service";
        HiveConfigParam hiveConfigParam = new HiveConfigParam(url, user, "", 40, 100, 3, 0);
        hiveClient = new HiveClient(hiveConfigParam);
        Assert.assertEquals(hiveClient.getTableLocation(tableName),metaStoreClient.getTableLocation(tableName));
    }

    @Test
    public void getColumnNamesTest() throws HiveClientException, TException, TableNotFoundException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://0.0.0.0:9083");
        hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        metaStoreClient = new MetaStoreClient(hiveMetaStoreClient);
        final String url = "jdbc:hive2://prod-hadoop-hadoopcluster2-jn-0001:2181,prod-hadoop-hadoopcluster2-jn-0002:2181,prod-hadoop-hadoopcluster2-jn-0003:2181,prod-hadoop-hadoopcluster2-jn-0004:2181,prod-hadoop-hadoopcluster2-jn-0005:2181/;transportMode=http;httpPath=cliservice;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hive_hadoopcluster";
        final String user = "fk-ip-data-service";
        HiveConfigParam hiveConfigParam = new HiveConfigParam(url, user, "", 40, 100, 3, 0);
        hiveClient = new HiveClient(hiveConfigParam);
        System.out.println(hiveClient.getColumnNames(tableName)+"="+metaStoreClient.getColumnNames(tableName));
        Assert.assertEquals(hiveClient.getColumnNames(tableName),metaStoreClient.getColumnNames(tableName));
    }
    @Test
    public void getHiveTableStorageFormatTest() throws HiveClientException, TException, TableNotFoundException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://0.0.0.0:9083");
        hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        metaStoreClient = new MetaStoreClient(hiveMetaStoreClient);
        final String url = "jdbc:hive2://prod-hadoop-hadoopcluster2-jn-0001:2181,prod-hadoop-hadoopcluster2-jn-0002:2181,prod-hadoop-hadoopcluster2-jn-0003:2181,prod-hadoop-hadoopcluster2-jn-0004:2181,prod-hadoop-hadoopcluster2-jn-0005:2181/;transportMode=http;httpPath=cliservice;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hive_hadoopcluster";
        final String user = "fk-ip-data-service";
        HiveConfigParam hiveConfigParam = new HiveConfigParam(url, user, "", 40, 100, 3, 0);
        hiveClient = new HiveClient(hiveConfigParam);
        System.out.println(hiveClient.getHiveTableStorageFormat(tableName)+"="+metaStoreClient.getHiveTableStorageFormat(tableName));
        Assert.assertEquals(hiveClient.getHiveTableStorageFormat(tableName),metaStoreClient.getHiveTableStorageFormat(tableName));
    }
    @Test
    public void getHiveTableFieldDelimitTest() throws HiveClientException, TException, TableNotFoundException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://0.0.0.0:9083");
        hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        metaStoreClient = new MetaStoreClient(hiveMetaStoreClient);
        final String url = "jdbc:hive2://prod-hadoop-hadoopcluster2-jn-0001:2181,prod-hadoop-hadoopcluster2-jn-0002:2181,prod-hadoop-hadoopcluster2-jn-0003:2181,prod-hadoop-hadoopcluster2-jn-0004:2181,prod-hadoop-hadoopcluster2-jn-0005:2181/;transportMode=http;httpPath=cliservice;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hive_hadoopcluster";
        final String user = "fk-ip-data-service";
        HiveConfigParam hiveConfigParam = new HiveConfigParam(url, user, "", 40, 100, 3, 0);
        hiveClient = new HiveClient(hiveConfigParam);
        System.out.println(hiveClient.getHiveTableFieldDelimit(tableName)+"="+metaStoreClient.getHiveTableFieldDelimit(tableName));
        Assert.assertEquals(hiveClient.getHiveTableFieldDelimit(tableName).toString(),metaStoreClient.getHiveTableFieldDelimit(tableName).toString());
    }
    @Test
    public void getColumnsWithTypeTest() throws HiveClientException, TException, TableNotFoundException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://0.0.0.0:9083");
        hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        metaStoreClient = new MetaStoreClient(hiveMetaStoreClient);
        final String url = "jdbc:hive2://prod-hadoop-hadoopcluster2-jn-0001:2181,prod-hadoop-hadoopcluster2-jn-0002:2181,prod-hadoop-hadoopcluster2-jn-0003:2181,prod-hadoop-hadoopcluster2-jn-0004:2181,prod-hadoop-hadoopcluster2-jn-0005:2181/;transportMode=http;httpPath=cliservice;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hive_hadoopcluster";
        final String user = "fk-ip-data-service";
        HiveConfigParam hiveConfigParam = new HiveConfigParam(url, user, "", 40, 100, 3, 0);
        hiveClient = new HiveClient(hiveConfigParam);
        Assert.assertEquals(hiveClient.getColumnsWithType(tableName),metaStoreClient.getColumnsWithType(tableName));
    }
    @Test
    public void getPartitionedColumnNamesTest() throws HiveClientException, TException, TableNotFoundException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://0.0.0.0:9083");
        hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        metaStoreClient = new MetaStoreClient(hiveMetaStoreClient);
        final String url = "jdbc:hive2://prod-hadoop-hadoopcluster2-jn-0001:2181,prod-hadoop-hadoopcluster2-jn-0002:2181,prod-hadoop-hadoopcluster2-jn-0003:2181,prod-hadoop-hadoopcluster2-jn-0004:2181,prod-hadoop-hadoopcluster2-jn-0005:2181/;transportMode=http;httpPath=cliservice;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hive_hadoopcluster";
        final String user = "fk-ip-data-service";
        HiveConfigParam hiveConfigParam = new HiveConfigParam(url, user, "", 40, 100, 3, 0);
        hiveClient = new HiveClient(hiveConfigParam);
        Assert.assertEquals(hiveClient.getPartitionedColumnNames(tableName),metaStoreClient.getPartitionedColumnNames(tableName));
    }
    @Test
    public void getHiveTableDetailsTest() throws HiveClientException, TException, TableNotFoundException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://0.0.0.0:9083");
        hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        metaStoreClient = new MetaStoreClient(hiveMetaStoreClient);
        final String url = "jdbc:hive2://prod-hadoop-hadoopcluster2-jn-0001:2181,prod-hadoop-hadoopcluster2-jn-0002:2181,prod-hadoop-hadoopcluster2-jn-0003:2181,prod-hadoop-hadoopcluster2-jn-0004:2181,prod-hadoop-hadoopcluster2-jn-0005:2181/;transportMode=http;httpPath=cliservice;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hive_hadoopcluster";
        final String user = "fk-ip-data-service";
        HiveConfigParam hiveConfigParam = new HiveConfigParam(url, user, "", 40, 100, 3, 0);
        hiveClient = new HiveClient(hiveConfigParam);
        HiveTableDetails hiveTableDetails = hiveClient.getHiveTableDetails(tableName);
        HiveTableDetails hiveTableDetails1 = metaStoreClient.getHiveTableDetails(tableName);
        Assert.assertEquals(hiveClient.getHiveTableDetails(tableName),metaStoreClient.getHiveTableDetails(tableName));
    }

}
