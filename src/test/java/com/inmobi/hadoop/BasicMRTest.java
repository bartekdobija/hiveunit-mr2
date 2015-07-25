package com.inmobi.hadoop;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.test.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BasicMRTest {

  private static final String CLUSTER_1 = "cluster1";

  private File testDataPath;
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileSystem fs;

  @Before
  public void setUp() throws Exception {
      testDataPath = new File(PathUtils.getTestDir(getClass()),
              "miniclusters");

      System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
      conf = new HdfsConfiguration();

      File testDataCluster1 = new File(testDataPath, CLUSTER_1);
      String c1Path = testDataCluster1.getAbsolutePath();
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1Path);
      cluster = new MiniDFSCluster.Builder(conf).build();

      fs = FileSystem.get(conf);
  }

  @After
  public void tearDown() throws Exception {
      Path dataDir = new Path(
              testDataPath.getParentFile().getParentFile().getParent());
      fs.delete(dataDir, true);
      File rootTestFile = new File(testDataPath.getParentFile().getParentFile().getParent());
      String rootTestDir = rootTestFile.getAbsolutePath();
      Path rootTestPath = new Path(rootTestDir);
      LocalFileSystem localFileSystem = FileSystem.getLocal(conf);
      localFileSystem.delete(rootTestPath, true);
      cluster.shutdown();
  }

  @Test
  public void testClusterWithData() throws Throwable {

      String IN_DIR = "testing/wordcount/input";
      String OUT_DIR = "testing/wordcount/output";
      String DATA_FILE = "sample.txt";

      FileSystem fs = FileSystem.get(conf);
      Path inDir = new Path(IN_DIR);
      Path outDir = new Path(OUT_DIR);

      fs.delete(inDir, true);
      fs.delete(outDir,true);

      // create the input data files
      List<String> content = new ArrayList<String>();
      content.add("She sells seashells at the seashore, and she sells nuts in the mountain.");
      writeHDFSContent(fs, inDir, DATA_FILE, content);

      // set up the job, submit the job and wait for it complete
      Job job = new Job(conf, "mr test wordcount");
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setMapperClass(BasicWordCount.TokenizerMapper.class);
      job.setReducerClass(BasicWordCount.IntSumReducer.class);
      FileInputFormat.addInputPath(job, inDir);
      FileOutputFormat.setOutputPath(job, outDir);
      job.waitForCompletion(true);
      assertTrue(job.isSuccessful());

      // now check that the output is as expected
      List<String> results = getJobResults(fs, outDir, 11);
      assertTrue(results.contains("She\t1"));
      assertTrue(results.contains("sells\t2"));

      // clean up after test case
      fs.delete(inDir, true);
      fs.delete(outDir,true);
  }


  private void writeHDFSContent(final FileSystem fs, final Path dir, final String fileName, final List<String> content) throws IOException {
      Path newFilePath = new Path(dir, fileName);
      FSDataOutputStream out = fs.create(newFilePath);
      for (String line : content){
          out.writeBytes(line);
      }
      out.close();
  }

  protected List<String> getJobResults(final FileSystem fs, final Path outDir, final int numLines) throws Exception {
      List<String> results = new ArrayList<String>();
      FileStatus[] fileStatus = fs.listStatus(outDir);
      for (FileStatus file : fileStatus) {
          String name = file.getPath().getName();
          if (name.contains("part-r-00000")){
              Path filePath = new Path(outDir + "/" + name);
              BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
              for (int i=0; i < numLines; i++){
                  String line = reader.readLine();
                  if (line == null){
                      fail("Results are not what was expected");
                  }
                  results.add(line);
              }
              assertNull(reader.readLine());
              reader.close();
          }
      }
      return results;
  }

}
