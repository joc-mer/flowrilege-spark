package exploration;

import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author joc
 */
public class PrintColumnsCount {

    public static void main(String... args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("GDELT events")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        sc.setLogLevel("OFF");

        //Count tabs in each lines
        JavaPairRDD<Integer, Long> tabCounts = sc.textFile("src/main/resources/gdelt-samples/head1000_of_20160308.gkg.csv")
                .mapToPair(line -> {
                    //Count characters is much more GC friendly than split and count
                    int cpt = 0;
                    for (int i = 0;i < line.length(); ++i) {
                        if (line.charAt(i) == '\t') {
                            ++cpt;
                        }
                    }
                    return new Tuple2<>(cpt, 1L);
                });

        //Count lines groupped by tab count
        JavaPairRDD<Integer, Long> lineCountByTabNumber = tabCounts.reduceByKey((left, right) -> left + right);

        //Swap key <-> value to allow sort on count result
        JavaPairRDD<Long, Integer> swappedLineCountByTabNumber = lineCountByTabNumber.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        
        //Sort and collect (no need to swap back to key -> value, it will be done in final loop
        List<Tuple2<Long, Integer>> collected = swappedLineCountByTabNumber.sortByKey(false).collect();
        
        System.out.println("Column count | line count");
        for(Tuple2<Long, Integer> tuple : collected) {
            System.out.println(tuple._2 + " | " + tuple._1);
        }
    }
    
}
