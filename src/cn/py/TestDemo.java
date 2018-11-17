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
	 * �����ļ�
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	@Test
	public void connect() throws IOException, URISyntaxException {
		//--��ȡHadoop������������
		Configuration conf = new Configuration();
		//--����ͨ���˶������趨����ͨ�������趨�����ȼ���Ҫ�����ļ��趨�����ȼ�
		conf.set("dfs.replication", "1");
		//--����hdfs�ļ�ϵͳ
		FileSystem system = FileSystem.get(new URI("hdfs://192.168.80.72:9000"),conf);
		//ָ��hdfsָ���ļ���������
		InputStream in = system.open(new Path("/park01/1.txt"));
		//��ȡ���ص��ļ������
		OutputStream out = new FileOutputStream(new File("data.txt"));
		//ͨ��hadoop�ṩ�Ĺ����࣬������ݴ���
		IOUtils.copyBytes(in, out, conf);
	}
	/**
	 * �ϴ��ļ�
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	@Test
	public void put() throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		FileSystem system = FileSystem.get(new URI("hdfs://192.168.80.72:9000"),conf);
		//��ȡhdfs���ļ������
		OutputStream out = system.create(new Path("/park02/2.txt"));
		//��ȡ�����ļ�������
		InputStream in = new FileInputStream(new File("data.txt"));
		//������ݴ���
		IOUtils.copyBytes(in, out, conf);
	}
	
	@Test
	public void other() throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		FileSystem system = FileSystem.get(new URI("hdfs://192.168.80.72:9000"),conf);
		
		//ɾ��ָ��Ŀ¼���ļ���true��ʾ�ݹ�ɾ�����൱��hdfs fs -rmr /park02
//		system.delete(new Path("/park02"),true);
		//Ŀ¼������
		system.rename(new Path("/park01"), new Path("/park02"));
	}
	
	@Test
	public void geyBlock() throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		FileSystem system = FileSystem.get(new URI("hdfs://192.168.80.72:9000"),conf);
		//�鿴һ��hdfs�����п���Ϣ
		BlockLocation[] blks = system.getFileBlockLocations(new Path("/park02/1.txt"), 0, Integer.MAX_VALUE);
		for (BlockLocation blockLocation : blks) {
			System.out.println(blockLocation);
		}
	}
}
