package spark.batch.questions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

public class AgeStatisticsQuestion implements Question, Serializable {
    private final String outputPath;
    private final SparkSession spark;

    // Define a static comparator function for min and max age
    private static final Comparator<Integer> ageComparator = Comparator.naturalOrder();

    public AgeStatisticsQuestion(SparkSession spark, String outputPath) {
        this.spark = spark;
        this.outputPath = outputPath;
    }

    @Override
    public void answer(JavaRDD<String> lines) {
        // Extract age information and calculate statistics
        JavaRDD<Integer> agesRDD = lines.map(line -> {
            String[] parts = line.split(",");
            // Assuming age is the second element in each line
            return Integer.parseInt(parts[1].replaceAll("\"", ""));
        });

        long totalCustomers = agesRDD.count();
        double meanAge = agesRDD.mapToDouble(age -> age).mean();// Replace Integer::compareTo with an anonymous function for minAge
        int minAge = agesRDD.min(ageComparator);
    
        // Replace Integer::compareTo with an anonymous function for maxAge
        int maxAge = agesRDD.max(ageComparator);
        

        // Prepare statistics as a single string
        String statisticsString = String.format(
                "Total Customers: %d\nMean Age: %.2f\nMinimum Age: %d\nMaximum Age: %d",
                totalCustomers, meanAge,  minAge, maxAge
        );

        // Write statistics to output file error line 47 !!!!!!!!!
        JavaRDD<Row> statisticsRDD = spark.createDataFrame(Arrays.asList(statisticsString), String.class).javaRDD();
        statisticsRDD.repartition(1).saveAsTextFile(outputPath + "/stats/age_statistics");
    }
}
