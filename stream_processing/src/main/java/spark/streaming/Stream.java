package spark.streaming;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.functions;

public class Stream {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession
            .builder()
            .appName("StreamProcessing")
            .master("local[*]")
            .getOrCreate();

        // Define the schema of the streaming DataFrame
        String schema = "id INT, age INT, gender STRING, income INT, education STRING, region STRING, " +
                        "loyalty_status STRING, purchase_frequency STRING, purchase_amount INT, " +
                        "product_category STRING, promotion_usage INT, satisfaction_score INT";

        // Read streaming data from the socket
        Dataset<Row> streamingDataFrame = spark
            .readStream()
            .format("socket")
            .option("host", "172.27.145.119")  // Replace with the IP address of your server
            .option("port", 9999)
            .load()
            .selectExpr("split(value, ',') as data")
            .selectExpr(
                "cast(data[0] as int) as id",
                "cast(data[1] as int) as age",
                "data[2] as gender",
                "cast(data[3] as int) as income",
                "data[4] as education",
                "data[5] as region",
                "data[6] as loyalty_status",
                "data[7] as purchase_frequency",
                "cast(data[8] as int) as purchase_amount",
                "data[9] as product_category",
                "cast(data[10] as int) as promotion_usage",
                "cast(data[11] as int) as satisfaction_score"
            );

        // Perform stream processing to calculate the total amount bought for each product category
        Dataset<Row> result = streamingDataFrame
            .groupBy("product_category")
            .agg(functions.sum("purchase_amount").alias("total_purchase_amount"))
            .orderBy("product_category");

        // Start the query to output the result to the console
        StreamingQuery query = result
            .writeStream()
            .outputMode("complete")
            .format("console")
            .start();

        // Wait for the query to terminate
        query.awaitTermination();
    }
}
