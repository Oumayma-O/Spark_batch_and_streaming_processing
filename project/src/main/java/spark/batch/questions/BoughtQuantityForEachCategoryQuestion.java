package spark.batch.questions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;


public class BoughtQuantityForEachCategoryQuestion implements Question {

    private final String outputPath;

    public BoughtQuantityForEachCategoryQuestion(String outputPath) {
        this.outputPath = outputPath;
    }

    @Override()
    public void answer(JavaRDD<String> lines) {

        JavaPairRDD<String, Integer> result = lines
                .mapToPair(line -> new Tuple2<>(line.split(",")[9], 1))
                .reduceByKey(Integer::sum); 
        result.saveAsTextFile(outputPath + "/purchase_analysis_trends/bought_quantity_for_each_category");
    }
}
