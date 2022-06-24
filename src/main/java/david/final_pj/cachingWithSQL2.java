package david.final_pj;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;

/**
 *
 * Measuring performance without cache, with cache, and with checkpoint (eager or not)
 *
 * Can be run via the command line: (same as textbook)
 *
 * ~/spark-2.4.7/bin/spark-submit  \
 *   --master spark://localhost:7077  \
 *   --class david.final_pj.cachingWithSQL2  \
 *   /home/ubuntu/myApp/final_pj.jar hdfs://localhost:9000/user/ubuntu/spark_input 2
 *                                                       |                         |
 *                                                       |                         +--  different testing mode
 *                                                       |
 *                                                       +--  INPUT
 *
 */


public class cachingWithSQL2 {
    enum Mode {
        NO_CACHE_NO_CHECKPOINT, CACHE, CHECKPOINT, CHECKPOINT_NON_EAGER, CHECKPOINT_WITH_CACHE
    }

    private SparkSession spark;

    Dataset<Row> df;


    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Not a valid command!!!!!!!!!!");
            return;
        }

        cachingWithSQL2 app = new cachingWithSQL2();
        app.start(args);

    }

    /**
     * The processing code.
     */
    private void start(String[] args) {
        // Creates a session on a local master
        this.spark = SparkSession.builder()
                .appName("Experiment of cache and checkpoint B")
//                .master("local[*]")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.memory", "4g")
                .config("spark.memory.offHeap.enabled", true)
                .config("spark.memory.offHeap.size", "4g")
                .getOrCreate();
        SparkContext sc = spark.sparkContext();
        sc.setCheckpointDir("/tmp");

        // cache -> NONE
        // checkpoint -> NONE
        // checkpoint_no_eager -> NONE
        // cache with checkpoint -> NONE
        // NONE

        df = spark.read().format("csv")
                .option("header", "true")
                .load(args[0]);


        int state = Integer.parseInt(args[1]);

        if (state == 0) {

            // Create and process the records with cache
            long t1 = processDataframe(Mode.CACHE);

            // Create and process the records without cache or checkpoint
            long t2 = processDataframe(Mode.NO_CACHE_NO_CHECKPOINT);

            spark.stop();

            System.out.println("\nProcessing times");
            System.out.println("With cache .................. " + t1 + " ms");
            System.out.println("Second entry................. " + t2 + " ms");

        } else if (state == 1) {

            // Create and process the records with a checkpoint
            long t1 = processDataframe(Mode.CHECKPOINT);

            // Create and process the records without cache or checkpoint
            long t2 = processDataframe(Mode.NO_CACHE_NO_CHECKPOINT);

            spark.stop();

            System.out.println("\nProcessing times");
            System.out.println("With checkpoint ............... " + t1 + " ms");
            System.out.println("Second entry................. " + t2 + " ms");

        } else if (state == 2) {

            // Create and process the records with a checkpoint
            long t1 = processDataframe(Mode.CHECKPOINT_NON_EAGER);

            // Create and process the records without cache or checkpoint
            long t2 = processDataframe(Mode.NO_CACHE_NO_CHECKPOINT);

            spark.stop();

            System.out.println("\nProcessing times");
            System.out.println("With checkpoint no eager .... " + t1 + " ms");
            System.out.println("Second entry................. " + t2 + " ms");

        } else if (state == 3) {

            // Create and process the records with a checkpoint and cache
            long t1 = processDataframe(Mode.CHECKPOINT_WITH_CACHE);

            // Create and process the records without cache or checkpoint
            long t2 = processDataframe(Mode.NO_CACHE_NO_CHECKPOINT);

            spark.stop();

            System.out.println("\nProcessing times");
            System.out.println("With checkpoint and cache ... " + t1 + " ms");
            System.out.println("Second entry................. " + t2 + " ms");

        }else {
            // Create and process the records without cache or checkpoint
            long t0 = processDataframe(Mode.NO_CACHE_NO_CHECKPOINT);

            spark.stop();

            System.out.println("\nProcessing times");
            System.out.println("Without cache ............... " + t0 + " ms");

        }
    }


    private long processDataframe(Mode mode) {

        long t0 = System.currentTimeMillis();
        Dataset<Row> topDf = df.filter(col("Severity").equalTo(2));
        switch (mode) {
            case CACHE:
                topDf = topDf.cache();
                break;

            case CHECKPOINT:
                topDf = topDf.checkpoint();
                break;

            case CHECKPOINT_NON_EAGER:
                topDf = topDf.checkpoint(false);
                break;

            case CHECKPOINT_WITH_CACHE:
                topDf = topDf.cache();
                topDf = topDf.checkpoint();
                break;

        }

        Dataset<Row> StateDf =
                topDf.groupBy("State").count().orderBy("State").sort(desc("count"));
        Dataset<Row> cityDf =
                topDf.groupBy("City").count().orderBy("City").sort(desc("count"));
        Dataset<Row> distanceDf =
                topDf.orderBy("Distance(mi)").sort(desc("Distance(mi)"));
        Dataset<Row> sunriseDf =
                topDf.groupBy("Sunrise_Sunset").count().orderBy("Sunrise_Sunset").sort(desc("count"));
        Dataset<Row> weatherDf =
                topDf.groupBy("Weather_Condition").count().orderBy("Weather_Condition").sort(desc("count"));
        long d1 = topDf.filter(col("Distance(mi)").between(0,5)).count();
        long d2 = topDf.filter(col("Distance(mi)").between(5,10)).count();
        long d3 = topDf.filter(col("Distance(mi)").geq(10)).count();

        System.out.println("10 States with most mild accidents happened in five years");
        StateDf.show(10);

        System.out.println("10 Cities with most mild accidents happened in five years");
        cityDf.show(10);

        System.out.println("10 LONGEST CONGESTION happened in five years");
        distanceDf.show(10);

        System.out.println("CONGESTION 0-5 mi:\t" + d1);
        System.out.println("CONGESTION 5-10 mi:\t" + d2);
        System.out.println("CONGESTION >10 mi:\t" + d3);

        System.out.println("SUNRISE or not");
        sunriseDf.show();

        System.out.println("Weather condition");
        weatherDf.show();

        long t1 = System.currentTimeMillis();
        System.out.println("Processing took " + (t1 - t0) + " ms.");


        return t1 - t0;
    }

}
