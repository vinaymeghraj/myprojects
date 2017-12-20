package hdfs;

import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LineReader;
import com.mapr.fs.MapRFileSystem;

/**
 * @author Vinayaka Meghraj
 * Command to execute this program
 * mvn exec:exec -Dexec.executable="java" -Dexec.args="-classpath %classpath -verbose:class hdfs.CreateDirectory" >> out2.txt 2>&1
 * java -cp $(hadoop classpath):YuCodesJava/target/YuCodesJava-0.0.1-SNAPSHOT.jar hdfs.CreateDirectory
 *
 */
public class CreateDirectory
{
	public static void main(String args[]) throws Exception {
		byte buf[] = new byte[ 65*1024];

		// String uri = "maprfs:///";
		String dirname = "";

		Configuration conf = new Configuration();
		String clusterName = "FiveTwo";
		String cldb1node = "10.10.70.180";
		//conf.setBoolean("fs.mapr.impl.clustername.unique", true);
		//conf.set("dfs.nameservices", clusterName);
		//conf.set("dfs.ha.namenodes." + clusterName, "cldb1");
		//conf.set("dfs.namenode.rpc-address." + clusterName + ".cldb1",cldb1node + ":7222");

		//FileSystem fs = FileSystem.get(URI.create(uri), conf); // if wanting to use a different cluster
		FileSystem fs = FileSystem.get(conf);

		Path dirpath = new Path( dirname + "/dir");
		Path wfilepath = new Path( dirname + "/file.w");
		//Path rfilepath = new Path( dirname + "/file.r");
		Path rfilepath = wfilepath;


		// Create Directory
		boolean res = fs.mkdirs( dirpath);
		if (!res) {
			System.out.println("Create Directory - mkdir failed for path: " + dirpath);
			return;
		}

		System.out.println( "Create Directory - mkdir( " + dirpath + ") successful, now writing file");

		// Create Write file
		FSDataOutputStream dataOutputStream = fs.create( wfilepath,
				true, // overwrite
				512, // buffersize
				(short) 1, // replication
				(long)(64*1024*1024) // chunksize
				);
		dataOutputStream.write(buf);
		dataOutputStream.close();
		MapRFileSystem mfs = new MapRFileSystem();

		System.out.println( "Create File - write( " + wfilepath + ") successful");

		// read rfile
		System.out.println( "Reading File: " + rfilepath);
		FSDataInputStream dataInputStream = fs.open( rfilepath);

		LineReader lineReader = new LineReader(dataInputStream, conf);
		Text line = new Text();
		lineReader.readLine(line);
		try {
			lineReader.close();
		} catch(IOException e){
		}
		String readLine = line.toString();
		String[] arr = readLine.split(" ");

		for(int k=1;k<arr.length;k++) System.out.println("Line  "+k+" Value is "+arr[k]);

		dataInputStream.close();
		System.out.println( "Read ok");
	}
}

