package spark.batch.questions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.Serializable;

public class CustomerSatisfactionQuestion implements Question, Serializable {
    private final String outputPath;

    public CustomerSatisfactionQuestion(String outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public void answer(JavaRDD<String> lines) {
        JavaPairRDD<String, Tuple2<Integer, Integer>> result = lines
            .mapToPair(line -> {
                String[] parts = line.split(",");
                String region = parts[5]; 
                String educationLevel = parts[4]; 
                int satisfactionScore = Integer.parseInt(parts[11].replaceAll("\"$", ""));
                return new Tuple2<>(region + "-" + educationLevel, new Tuple2<>(satisfactionScore, 1));
            })
            .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
            .mapValues(sumCount -> sumCount);
    
        JavaPairRDD<String, Double> averageSatisfaction = result
            .mapToPair(pair -> {
                String regionEducation = pair._1;
                int sumSatisfaction = pair._2._1;
                int numCustomers = pair._2._2;
                double average = sumSatisfaction / (double) numCustomers;
                return new Tuple2<>(regionEducation, average);
            });
    
        averageSatisfaction.saveAsTextFile(outputPath + "/average_satisfaction_by_region_education");
    }
    
}
