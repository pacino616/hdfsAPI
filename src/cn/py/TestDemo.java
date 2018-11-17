package cn.py;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import com.google.gson.JsonObject;

public class TestDemo {
	/**
	 * 下载文件
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	@Test
	public void connect() throws IOException, URISyntaxException {
		//--获取Hadoop环境变量对象
		Configuration conf = new Configuration();
		//--可以通过此对象来设定对象，通过代码设定的优先级，要高于文件设定的优先级
		conf.set("dfs.replication", "1");
		//--链接hdfs文件系统
		FileSystem system = FileSystem.get(new URI("hdfs://192.168.80.72:9000"),conf);
		//指定hdfs指定文件的输入流
		InputStream in = system.open(new Path("/park01/1.txt"));
		//获取本地的文件输出流
		OutputStream out = new FileOutputStream(new File("data.txt"));
		//通过hadoop提供的工具类，完成数据传输
		IOUtils.copyBytes(in, out, conf);
	}
	/**
	 * 上传文件
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	@Test
	public void put() throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		FileSystem system = FileSystem.get(new URI("hdfs://192.168.80.72:9000"),conf);
		//获取hdfs的文件输出流
		OutputStream out = system.create(new Path("/park02/2.txt"));
		//获取本地文件输入流
		InputStream in = new FileInputStream(new File("data.txt"));
		//完成数据传输
		IOUtils.copyBytes(in, out, conf);
	}
	
	@Test
	public void other() throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		FileSystem system = FileSystem.get(new URI("hdfs://192.168.80.72:9000"),conf);
		
		//删除指定目录或文件，true表示递归删除，相当于hdfs fs -rmr /park02
//		system.delete(new Path("/park02"),true);
		//目录重命名
		system.rename(new Path("/park01"), new Path("/park02"));
	}
	
	@Test
	public void geyBlock() throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		FileSystem system = FileSystem.get(new URI("hdfs://192.168.80.72:9000"),conf);
		//查看一个hdfs的所有块信息
		BlockLocation[] blks = system.getFileBlockLocations(new Path("/park02/1.txt"), 0, Integer.MAX_VALUE);
		for (BlockLocation blockLocation : blks) {
			System.out.println(blockLocation);
		}
	}
}
