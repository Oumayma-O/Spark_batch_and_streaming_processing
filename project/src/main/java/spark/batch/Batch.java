package spark.batch;

import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import spark.batch.questions.BoughtQuantityForEachCategoryQuestion;
import spark.batch.questions.CustomerSatisfactionQuestion;
import spark.batch.questions.PromotionalOffersQuestion;
import spark.batch.questions.TotalAmountSpentByCategoryQuestion;
import spark.batch.questions.TotalAmountSpentByGenderQuestion;
import spark.batch.questions.TotalAmountSpentByRegionQuestion;

public class Batch {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: Main <input-path> <output-path>");
            System.exit(1);
        }
    
        String inputPath = args[0];
        String outputPath = args[1];
    
        SparkSession spark = SparkSession.builder()
                .appName("BatchProcessingCustomerPurchaseBehaviourEDA")
                .getOrCreate();
    
        try {
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
            Path outputDir = new Path(outputPath);
            if (fs.exists(outputDir)) {
                fs.delete(outputDir, true); 
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    
        BatchProcessor batchProcessor = new BatchProcessor(spark, inputPath);
        batchProcessor.addQuestion(new BoughtQuantityForEachCategoryQuestion(outputPath));
        batchProcessor.addQuestion(new TotalAmountSpentByCategoryQuestion(outputPath));
        batchProcessor.addQuestion(new TotalAmountSpentByRegionQuestion(outputPath));
        batchProcessor.addQuestion(new TotalAmountSpentByGenderQuestion(outputPath));
        batchProcessor.addQuestion(new PromotionalOffersQuestion(outputPath));
        batchProcessor.addQuestion(new CustomerSatisfactionQuestion(outputPath));

    
        batchProcessor.process();
    
        spark.stop();
    }
    

}
