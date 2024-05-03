package spark.batch.questions;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.Serializable;

public class PromotionalOffersQuestion implements Question, Serializable {
    private final String outputPath;

    public PromotionalOffersQuestion( String outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public void answer(JavaRDD<String> lines) {
        JavaPairRDD<Integer, Long> result = lines
            .mapToPair(line -> {
                String[] parts = line.split(",");
                int promotionUsage = Integer.parseInt(parts[10]); 
                return new Tuple2<>(promotionUsage, 1L);
            })
            .reduceByKey(Long::sum);

        result.saveAsTextFile(outputPath + "/promotional_offers_count");
    }
}
