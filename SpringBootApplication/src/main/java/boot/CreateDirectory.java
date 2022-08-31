package boot;

import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class CreateDirectory {

    public static com.mapr.fs.MapRFileSystem getFileSystemValue() {

        com.mapr.fs.MapRFileSystem fileSystem = null;
        try {
            Configuration conf = new Configuration();
            //FileSystem fs = FileSystem.get(conf);
            //TODO Change url with hdfs name node path
            // conf.set("fs.defaultFS", "maprfs:///");
            // if (fileSystem == null)
//                fileSystem = FileSystem.get(conf);
            fileSystem = (MapRFileSystem) MapRFileSystem.get(conf);
            System.out.println("fileSystem value :: " + fileSystem);
            Path path = new Path("maprfs:///vinayk8s2/createdNewDir");
            fileSystem.create(path);
        } catch (Exception e) {
            e.printStackTrace();
            //  logger.info("Exception while fetching fileSystemValue :: " + e.getMessage());
            //  logger.error("Exception while fetching fileSystemValue :: ");
        }
        return fileSystem;
    }

    public static void main(String[] args) {

        getFileSystemValue();
    }
}
