package com.inmobi.hive.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class HiveScriptParseTest {

    @Test
    public void testSimpleCase() {
        String scriptFile = "src/test/resources/scripts/simple.hql";
        Map<String, String> params = null;
        List<String> excludes = null;
        HiveScript hiveScript = new HiveScript(scriptFile, params, excludes);
        List<String> statements = hiveScript.getStatements();
        assertEquals(2, statements.size());
        assertEquals("SELECT COUNT(*) FROM table1", statements.get(0));
        assertEquals("SELECT name, address FROM table2", statements.get(1));
    }

    @Test
    public void testWithComments() {
        String scriptFile = "src/test/resources/scripts/with_comments.hql";
        Map<String, String> params = null;
        List<String> excludes = null;
        HiveScript hiveScript = new HiveScript(scriptFile, params, excludes);
        List<String> statements = hiveScript.getStatements();
        assertEquals(2, statements.size());
        assertEquals("SELECT COUNT(*) FROM table1", statements.get(0));
        assertEquals("SELECT name, address FROM table2", statements.get(1));
    }

    @Test
    public void testMultiline() {
        String scriptFile = "src/test/resources/scripts/multiline.hql";
        Map<String, String> params = null;
        List<String> excludes = null;
        HiveScript hiveScript = new HiveScript(scriptFile, params, excludes);
        List<String> statements = hiveScript.getStatements();
        assertEquals(1, statements.size());
        String line1 = "CREATE TABLE raw_data (   name STRING,   " +
                "address STRING ) ROW FORMAT DELIMITED    FIELDS " +
                "TERMINATED BY ','    LINES TERMINATED BY '\\n' " +
                "STORED AS TEXTFILE";
        assertEquals(line1, statements.get(0));
    }

    @Test
    public void testParams() {
        String scriptFile = "src/test/resources/scripts/params.hql";
        Map<String, String> params = new HashMap<String, String>();
        params.put("nameNode", "hdfs://localhost:9000");
        params.put("MY_LIB", "/my-lib-1.0.0.jar");
        params.put("HDFS_PATH", "/user/hadoop/data");
        List<String> excludes = null;
        HiveScript hiveScript = new HiveScript(scriptFile, params, excludes);
        List<String> statements = hiveScript.getStatements();
        assertEquals(3, statements.size());
        String line1 = "ADD JAR hdfs://localhost:9000/my-lib-1.0.0.jar";
        String line2 = "ADD JAR hdfs://localhost:9000/user/hadoop/libs" +
                "/json-serde-1.3.jar";
        String line3 = "CREATE EXTERNAL TABLE table1 (   name STRING ) " +
                "STORED AS RCFILE LOCATION '/user/hadoop/data/rc'";
        assertEquals(line1, statements.get(0));
        assertEquals(line2, statements.get(1));
        assertEquals(line3, statements.get(2));
    }

    @Test
    public void testExcludeLines() {
        String scriptFile = "src/test/resources/scripts/excludes.hql";
        Map<String, String> params = null;
        List<String> excludes = new ArrayList<String>();
        excludes.add("ADD JAR ${MY_LIB};");
        HiveScript hiveScript = new HiveScript(scriptFile, params, excludes);
        List<String> statements = hiveScript.getStatements();
        assertEquals(1, statements.size());
        assertEquals("SELECT COUNT(*) FROM table1", statements.get(0));
    }

    @Test
    public void testSetLines() {
        String scriptFile = "src/test/resources/scripts/set.hql";
        Map<String, String> params = new HashMap<>();
        List<String> excludes = new ArrayList<>();
        HiveScript hiveScript = new HiveScript(scriptFile, params, excludes);
        List<String> statements = hiveScript.getStatements();
        assertEquals(2, statements.size());
        assertEquals("SET hive.support.quoted.identifiers=none", statements.get(0));
        assertEquals("set hive.exec.parallel=true", statements.get(1));
    }

    @Test
    public void testAddLines() {
        String scriptFile = "src/test/resources/scripts/add.hql";
        Map<String, String> params = new HashMap<>();
        List<String> excludes = new ArrayList<>();
        HiveScript hiveScript = new HiveScript(scriptFile, params, excludes);
        List<String> statements = hiveScript.getStatements();
        assertEquals(5, statements.size());
        assertEquals("add jar /test.jar", statements.get(0));
        assertEquals("ADD JAR /test.jar", statements.get(1));
        assertEquals("add file /test.txt", statements.get(2));
        assertEquals("add FILE /test.txt", statements.get(3));
        assertEquals("add ARCHIVES /test.zip", statements.get(4));
    }

    @Test
    public void testDfsLines() {
        String scriptFile = "src/test/resources/scripts/dfs.hql";
        Map<String, String> params = new HashMap<>();
        List<String> excludes = new ArrayList<>();
        HiveScript hiveScript = new HiveScript(scriptFile, params, excludes);
        List<String> statements = hiveScript.getStatements();
        assertEquals(2, statements.size());
        assertEquals("dfs -ls", statements.get(0));
        assertEquals("dfs -chown root:root /tmp/test.txt", statements.get(1));
    }

    @Test
    public void testUnionWithNestedSelect() {
        String scriptFile = "src/test/resources/scripts/union_nested.hql";
        Map<String, String> params = new HashMap<>();
        List<String> excludes = new ArrayList<>();

        HiveScript hiveScript = new HiveScript(scriptFile, params, excludes);
        List<String> statements = hiveScript.getStatements();
        assertEquals(5, statements.size());
        assertEquals("SELECT c.* FROM (   SELECT a.* FROM union_a a   "
                + "UNION ALL   SELECT b.* FROM union_b b WHERE b.year NOT IN "
                + "(SELECT year FROM union_a WHERE year = '1949') ) c",
                statements.get(4));
    }

    @Test
    public void testJoinedTables() {
        String scriptFile = "src/test/resources/scripts/joined_tables.hql";
        Map<String, String> params = new HashMap<>();
        List<String> excludes = new ArrayList<>();

        HiveScript hiveScript = new HiveScript(scriptFile, params, excludes);
        List<String> statements = hiveScript.getStatements();
        assertEquals(5, statements.size());
        assertEquals("SELECT * FROM join_a a JOIN join_b b ON a.temp = b.temp",
                statements.get(4));
    }

}
