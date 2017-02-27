package hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * <h1>HDFS Demo</h1>
 * <p>
 * </p>
 * 
 * @author 李春
 * @time 2017年2月16日 下午4:34:39
 */
public class DFFSDemo {
	private static FileSystem fileSystem;
	static {
		try {
			fileSystem = FileSystem.get(new URI("hdfs://192.168.80.128:9000/"),
					new Configuration(), "root");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

	public static void upload() throws Exception {
		InputStream in = new FileInputStream("F:/temp/file");
		OutputStream out = fileSystem.create(new Path("/mydata/file"));
		IOUtils.copyBytes(in, out, 4096, true);
	}

	public static void main(String[] args) throws IOException,
			URISyntaxException {
		System.setProperty("hadoop.home.dir", "/mydata/hadoop");
		FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"),
				new Configuration());
		InputStream in = fs.open(new Path("/jdk.avi"));
		FileOutputStream out = new FileOutputStream(new File("c:/jdk123456"));
		IOUtils.copyBytes(in, out, 2048, true);
	}

}
