package spark.batch;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import spark.batch.questions.Question;

import java.util.ArrayList;
import java.util.List;

public class BatchProcessor {

    private final SparkSession spark;
    private final String inputPath;
    private final List<Question> questions;

    public BatchProcessor(SparkSession spark, String inputPath) {
        this.spark = spark;
        this.inputPath = inputPath;
        this.questions = new ArrayList<>();
    }

    public void addQuestion(Question question) {
        questions.add(question);
    }

    public void process() {


        JavaRDD<String> lines = spark.read().textFile(inputPath).javaRDD();

        for (Question question : questions) {
            question.answer(lines);
        }
    }
}
