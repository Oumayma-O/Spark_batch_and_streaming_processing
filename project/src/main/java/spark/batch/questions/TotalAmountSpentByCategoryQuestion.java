package spark.batch.questions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class TotalAmountSpentByCategoryQuestion implements Question {

    private final String outputPath;

    public TotalAmountSpentByCategoryQuestion(String outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public void answer(JavaRDD<String> lines) {
        JavaPairRDD<String, Integer> result = lines
        .mapToPair(line -> {
            String[] parts = line.split(",");
            String category = parts[9];  // Category at index 9
            int amountSpent = Integer.parseInt(parts[8]);  // Purchase amount at index 8
            return new Tuple2<>(category, amountSpent);
        })
        .reduceByKey(Integer::sum);
    

        result.saveAsTextFile(outputPath + "/purchase_analysis_trends/total_amount_spent_by_category");
    }
}

