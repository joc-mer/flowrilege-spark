package exploration;

import java.io.Serializable;
import java.util.Comparator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 *
 * @author joc
 */
public class PrintUniqueColumns {

    public static void main(String... args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("GDELT events")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        
        //Count tabs in each lines
        JavaRDD<String[]> parts = sc.textFile("src/main/resources/gdelt-samples/head1000_of_20160308.gkg.csv")
                .map(line -> line.split("\t"));
        
        parts.cache();

        int i = 0;
        
        System.out.println("Column " + (i++) + " has unique values : " + columnHasUniqueValues(parts, i));
        System.out.println("Column " + (i++) + " has unique values : " + columnHasUniqueValues(parts, i));
        System.out.println("Column " + (i++) + " has unique values : " + columnHasUniqueValues(parts, i));
        System.out.println("Column " + (i++) + " has unique values : " + columnHasUniqueValues(parts, i));
        System.out.println("Column " + (i++) + " has unique values : " + columnHasUniqueValues(parts, i));
        System.out.println("Column " + (i++) + " has unique values : " + columnHasUniqueValues(parts, i));
        System.out.println("Column " + (i++) + " has unique values : " + columnHasUniqueValues(parts, i));
        System.out.println("Column " + (i++) + " has unique values : " + columnHasUniqueValues(parts, i));
        System.out.println("Column " + (i++) + " has unique values : " + columnHasUniqueValues(parts, i));
        System.out.println("Column " + (i++) + " has unique values : " + columnHasUniqueValues(parts, i));
    }
    
    static class NthColumn<T> implements Function<T[], T> {

        final int column;

        public NthColumn(int column) {
            this.column = column;
        }
        
        @Override
        public T call(T[] v1) throws Exception {
            return v1[column];
        }
        
    }
    
    static <T> boolean columnHasUniqueValues(JavaRDD<T[]> columnsRdd, int columnCount) {
        //Only select nth column
        JavaRDD<T> columnRdd = columnsRdd.map(new NthColumn<T >(columnCount));
        
        //For each value, start with 1 counter
        JavaPairRDD<T, Long> preCountRdd = columnRdd.mapToPair(val -> new Tuple2<>(val, 1L));
        
        //Actually count
        JavaPairRDD<T, Long> countsRdd = preCountRdd.reduceByKey((left, right) -> left + right);
        
        //Do the maximum of count
        long max = countsRdd.map(tuple -> tuple._2).max(new NativeLongComparator());
        
        //Si it only one occurence by value ?
        return max == 1L;
    }
 
    static class NativeLongComparator implements Comparator<Long>, Serializable {

        @Override
        public int compare(Long left, Long right) {
            return (left < right ? -1 : left > right ? 1 : 0);
        }
        
    }
}
