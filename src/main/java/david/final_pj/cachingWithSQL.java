package david.final_pj;

import david.final_pj.utils.RecordGeneratorUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.sparkproject.dmg.pmml.False;

import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 *
 * Measuring performance without cache, with cache, and with checkpoint (eager or not)
 *
 * Can be run via the command line: (same as textbook)
 *
 * ~/spark-2.4.7/bin/spark-submit  \
 *   --master spark://localhost:7077  \
 *   --class david.final_pj.cachingWithSQL  \
 *   /home/ubuntu/myApp/final_pj.jar 1000000 2
 *                                   |    |
 *                                   |    +--  different testing mode
 *                                   |
 *                                   +--  Number of records to create (only useful when arg[0] is 0)
 *
 *
 */


public class cachingWithSQL {
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

        int recordCount = Integer.parseInt(args[0]);
        int state = Integer.parseInt(args[1]);

        cachingWithSQL app = new cachingWithSQL();
        app.start(recordCount, state);

    }

    /**
     * The processing code.
     */
    private void start(int recordCount, int state) {
        System.out.printf("-> start(%d)\n", recordCount);

        // Creates a session on a local master
        this.spark = SparkSession.builder()
                .appName("Experiment of cache and checkpoint A")
//                .master("local[*]")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.memory", "4g")
                .config("spark.memory.offHeap.enabled", true)
                .config("spark.memory.offHeap.size", "4g")
                .getOrCreate();
        SparkContext sc = spark.sparkContext();
        sc.setCheckpointDir("/tmp");

        // NONE
        // cache -> NONE
        // checkpoint -> NONE
        // cache with checkpoint -> NONE (need manually set)
        // checkpoint_no_eager -> NONE

        df = RecordGeneratorUtils.createDataframe(this.spark, recordCount);

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
        Dataset<Row> topDf = df.filter(col("rating").equalTo(5));
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

        List<Row> langDf =
                topDf.groupBy("lang").count().orderBy("lang").collectAsList();
        List<Row> yearDf =
                topDf.groupBy("year").count().orderBy(col("year").desc())
                        .collectAsList();


        System.out.println("Five-star publications per language");
        for (Row r : langDf) {
            System.out.println(r.getString(0) + " ... " + r.getLong(1));
        }

        System.out.println("\nFive-star publications per year");
        for (Row r : yearDf) {
            System.out.println(r.getInt(0) + " ... " + r.getLong(1));
        }

        long t1 = System.currentTimeMillis();

        System.out.println("Processing took " + (t1 - t0) + " ms.");

        return t1 - t0;
    }

}
