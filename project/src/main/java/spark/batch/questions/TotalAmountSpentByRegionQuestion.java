package spark.batch.questions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class TotalAmountSpentByRegionQuestion implements Question {

    private final String outputPath;

    public TotalAmountSpentByRegionQuestion(String outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public void answer(JavaRDD<String> lines) {
        JavaPairRDD<String, Integer> result = lines
                .mapToPair(line -> {
                    String region = line.split(",")[5];
                    int amountSpent = Integer.parseInt(line.split(",")[8]);
                    return new Tuple2<>(region, amountSpent);
                })
                .reduceByKey(Integer::sum); 

        result.saveAsTextFile(outputPath + "/purchase_analysis_trends/total_amount_spent_by_region");
    }
}
