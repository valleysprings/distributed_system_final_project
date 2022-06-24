package david.final_pj.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * Utility methods to help generate random records.
 *
 * @author jgp
 */
public abstract class RecordGeneratorUtils {
    private static Calendar cal = Calendar.getInstance();

    private static String[] fnames = { "John", "Kevin", "Lydia", "Nathan",
            "Jane", "Liz", "Sam", "Ruby", "Peter", "Rob", "Mahendra", "Noah",
            "Noemie", "Fred", "Anupam", "Stephanie", "Ken", "Sam", "Jean-Georges",
            "Holden", "Murthy", "Jonathan", "Jean", "Georges", "Oliver" };
    private static String[] lnames = { "Smith", "Mills", "Perrin", "Foster",
            "Kumar", "Jones", "Tutt", "Main", "Haque", "Christie", "Karau",
            "Kahn", "Hahn", "Sanders" };
    private static String[] articles = { "The", "My", "A", "Your", "Their" };
    private static String[] descriptions = { "", "Great", "Beautiful", "Better",
            "Worse", "Gorgeous", "Terrific", "Terrible", "Natural", "Wild" };
    private static String[] nouns = { "Life", "Trip", "Experience", "Work",
            "Job", "Beach" };
    private static String[] lang = { "fr", "en", "es", "de", "it", "pt" };
    private static int[] daysInMonth =
            { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

    public static Dataset<Row> createDataframe(SparkSession spark,
                                               int recordCount) {
        System.out.println("-> createDataframe()");
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField(
                        "name",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "title",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "rating",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "year",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "lang",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "Article",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "SSN",
                        DataTypes.StringType,
                        false)
            }
        );

        Dataset<Row> df = null;
        int inc = 500000;
        long recordCreated = 0;
        while (recordCreated < recordCount) {
            long recordInc = inc;
            if (recordCreated + inc > recordCount) {
                recordInc = recordCount - recordCreated;
            }
            List<Row> rows = new ArrayList<>();
            for (long j = 0; j < recordInc; j++) {
                rows.add(RowFactory.create(
                                getFirstName() + " " + getLastName(),
                                getTitle(),
                                getRating(),
                                getRecentYears(25),
                                getLang(),
                                getArticle(),
                                getRandomSSN()

                        )
                );
            }
            if (df == null) {
                df = spark.createDataFrame(rows, schema);
            } else {
                df = df.union(spark.createDataFrame(rows, schema));
            }
            recordCreated = df.count();
            System.out.println(recordCreated + " records created");
        }

        assert df != null;
        df.show(3, false);
        System.out.println("<- createDataframe()");
        return df;
    }

    public static String getLang() {
        return lang[getRandomInt(lang.length)];
    }

    public static int getRecentYears(int i) {
        return cal.get(Calendar.YEAR) - getRandomInt(i);
    }

    public static int getRating() {
        return getRandomInt(5) + 1;
    }

    public static String getRandomSSN() {
        return "" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10) + "-"
                + getRandomInt(10) + getRandomInt(10)
                + "-" + getRandomInt(10) + getRandomInt(10) + getRandomInt(10)
                + getRandomInt(10);
    }

    public static int getRandomInt(int i) {
        return (int) (Math.random() * i);
    }

    public static String getFirstName() {
        return fnames[getRandomInt(fnames.length)];
    }

    public static String getLastName() {
        return lnames[getRandomInt(lnames.length)];
    }

    public static String getArticle() {
        return articles[getRandomInt(articles.length)];
    }

    public static String getDescriptions() {
        return descriptions[getRandomInt(descriptions.length)];
    }

    public static String getNoun() {
        return nouns[getRandomInt(nouns.length)];
    }

    public static String getTitle() {
        return (getArticle() + " " + getDescriptions()).trim() + " " + getNoun();
    }

}
