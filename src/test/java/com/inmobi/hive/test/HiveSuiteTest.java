package com.inmobi.hive.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HiveSuiteTest {

    private HiveTestSuite testSuite;

    @Before
    public void setUp() throws Exception {
        testSuite = new HiveTestSuite();
        testSuite.createTestCluster();
    }

    @After
    public void tearDown() throws Exception {
        testSuite.shutdownTestCluster();
    }

    @Test
    public void testManagedTable() throws Throwable {
        File inputRawData = new File("src/test/resources/files/weather.txt");
        String inputRawDataAbsFilePath = inputRawData.getAbsolutePath();
        Map<String, String> params = new HashMap<String, String>();
        params.put("WEATHER_DATA", inputRawDataAbsFilePath);

        List<String> results = testSuite.executeScript("src/test/resources/scripts/managed_weather.hql", params);
        assertEquals(1, results.size());
        assertEquals("2", results.get(0));
    }

    @Test
    public void testExternalTable() throws Throwable {
        FileSystem fs = testSuite.getFS();
        Path homeDir = fs.getHomeDirectory();
        String rawHdfsDirPath = homeDir + "/testing/input";
        Path rawHdfsData = new Path(rawHdfsDirPath + "/weather.txt");
        File inputRawData = new File("src/test/resources/files/weather.txt");
        String inputRawDataAbsFilePath = inputRawData.getAbsolutePath();
        Path inputData = new Path(inputRawDataAbsFilePath);
        fs.copyFromLocalFile(inputData, rawHdfsData);

        Map<String, String> params = new HashMap<String, String>();
        params.put("WEATHER_DATA", rawHdfsDirPath);
        List<String> results = testSuite.executeScript("src/test/resources/scripts/external_weather.hql", params);

        assertEquals(2, results.size());
        assertTrue(results.contains("1950\t22\t1"));
        assertTrue(results.contains("1949\t111\t1"));
    }

    @Test
    public void testQuotedIdentifiers() throws IOException {
        Map<String, String> params = new HashMap<String, String>();
        params.put("WEATHER_DATA",
            new File("src/test/resources/files/weather.txt").getAbsolutePath());
        List<String> results = testSuite.executeScript("src/test/resources/scripts/quoted_ids.hql", params);

        assertEquals(1, results.size());
        assertEquals("1950\t22", results.get(0));
    }

}
