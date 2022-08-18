package connector;

import com.mapr.db.spark.MapRDBSpark;
import com.mapr.db.spark.api.java.MapRDBJavaSparkContext;
import com.mapr.db.spark.impl.OJAIDocument;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/**
 * Command to submit the job
 *
 * * /opt/mapr/spark/spark-2.4.4/bin/spark-submit --master yarn --deploy-mode client --class connector.SaveToMapRDBJSON --num-executors 1 --executor-memory 1G --executor-cores 1 --driver-memory 1G --driver-cores 1 /home/mapr/apRDBSparkConnector-1.0-SNAPSHOT.jar /tmp/testData3 true
 */

public class SaveToMapRDBJSON {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("yarn").appName("Sample").getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        //String input = args[0];
        Boolean b = Boolean.valueOf(args[1]);
        System.out.println("Saving data to MapRDBJSON");
        JavaRDD<String> documents = jsc
                .parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
                .map(i -> { return "{\"_id\": \"" + i + "\", \"test\": \"" + i + "\"}"; });

        MapRDBJavaSparkContext a = new MapRDBJavaSparkContext(spark.sparkContext());
        documents.take(10).forEach(System.out::println);
        JavaRDD<OJAIDocument> maprd = documents.map(MapRDBSpark::newDocument);
        //a.saveToMapRDB(maprd, args[0], b, false, "_id");
        a.saveToMapRDB(maprd, args[0]);

    }
}