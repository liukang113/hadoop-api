package com.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * 客户端去操作hdfs时，是有一个用户身份的
 * 默认情况下，hdfs客户端api会从jvm中获取一个参数来作为自己的用户身份：-DHADOOP_USER_NAME=hadoop
 * <p>
 * 也可以在构造客户端fs对象时，通过参数传递进去
 *
 * @author
 */
public class HdfsClientDemo {
    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init()  throws IOException, InterruptedException, URISyntaxException {
        conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://node1:9000"), conf, "hadoop");
    }

    @Test
    public void testUpload() throws Exception {

//        Thread.sleep(2000);
        fs.copyFromLocalFile(new Path("C:\\Users\\ll\\Downloads\\node\\label.online.data"), new Path("/bigdata/test/label.online.data"));
        fs.close();
    }


    @Test
    public void testDownload() throws Exception {
        fs.copyToLocalFile(new Path("/bigdata/test/label.online.data"), new Path("d:/"));
        System.out.println("==========================下载数据完毕");
        fs.close();
    }

    @Test
    public void testConf() {
        Iterator<Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()) {
            Entry<String, String> entry = iterator.next();
            System.out.println(entry.getValue() + "--" + entry.getValue());//conf加载的内容
        }
    }

    /**
     * 创建目录
     */
    @Test
    public void makdirTest() throws Exception {
        boolean mkdirs = fs.mkdirs(new Path("/aaa/bbb"));
        System.out.println(mkdirs);
    }

    /**
     * 删除
     */
    @Test
    public void deleteTest() throws Exception {
        boolean delete = fs.delete(new Path("/aaa"), true);//true， 递归删除
        System.out.println(delete);
    }

    @Test
    public void listTest() throws Exception {

        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            System.err.println(fileStatus.getPath() + "=================" + fileStatus.toString());
        }
        //会递归找到所有的文件
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus next = listFiles.next();
            String name = next.getPath().getName();
            Path path = next.getPath();
            System.out.println(name + "---" + path.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:9000");
        //拿到一个文件系统操作的客户端实例对象
        FileSystem fs = FileSystem.get(conf);
        System.out.println("=======================上传数据");
//        fs.copyFromLocalFile(new Path("C:\\Users\\ll\\Downloads\\node\\label.online.data"), new Path("/bigdata/test/label.online.data"));
//        fs.close();
//        fs.copyToLocalFile(new Path("/bigdata/test/label.online.data"), new Path("d:\\"));
//        fs.close();
//        FileStatus[] listStatus = fs.listStatus(new Path("/"));
//        for (FileStatus fileStatus : listStatus) {
//            System.err.println(fileStatus.getPath() + "=================" + fileStatus.toString());
//        }
//        //会递归找到所有的文件
//        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
//        while (listFiles.hasNext()) {
//            LocatedFileStatus next = listFiles.next();
//            String name = next.getPath().getName();
//            Path path = next.getPath();
//            System.err.println(name + "=================" + path.toString());
//        }

        boolean delete = fs.delete(new Path("/aaa"), true);//true， 递归删除
        System.out.println("是否操作成功 %," + delete);
    }
}
