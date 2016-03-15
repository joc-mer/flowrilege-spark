package exploration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author joc
 */
public class PrintHTTPColumns {

    public static void main(String... args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("GDELT events")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        if(args.length == 0) {
            args = "/home/joc/Projets/dev/flowrilege-spark/src/main/resources/gdelt-samples/head1000_of_20160308.gkg.csv".split(" ");
        }
        
        try {
            CSVExploratorParam fromArgs = CSVExploratorParam.fromArgs(args);
            JavaRDD<String[]> splitedLines = sc.textFile(fromArgs.csvFile)
                    .zipWithIndex()
                    .filter((tuple) -> tuple._2 < fromArgs.limit).map(tuple -> tuple._1)
                    .map(line -> line.split("\t"));

            //Transformes arrays of string into long bitmask to quickly find columns where there is HTTP URLs
            //TODO : use BitSet for large CSV files
            JavaRDD<Long> httpUrlBitMask = splitedLines.map((chunks)
                    -> {
                long ret = 0L;
                long mask = 1L;
                for (String s : chunks) {
                    if (s.startsWith("http://")) {
                        ret |= mask;
                    }
                    mask = mask << 1;
                }

                return ret;
            });

            //Merges masks
            long reduced = httpUrlBitMask.reduce((left, right) -> left | right);

            for (int i = 0; i < 64; ++i) {
                long mask = 1L << i;
                if ((reduced & mask) != 0L) {
                    System.out.println("Column " + i + " contains URLs");
                }
            }
        } catch (CSVExploratorParam.InvalidArgumentException ex) {
            System.err.println(ex.getMessage());
            printHelp();
        }

    }

    public static void printHelp() {
        System.out.println("usage : expl-csv-http-col [-l <line limit>] <csv file>");
    }

}
