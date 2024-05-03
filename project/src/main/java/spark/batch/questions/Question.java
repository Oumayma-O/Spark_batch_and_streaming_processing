package spark.batch.questions;

import org.apache.spark.api.java.JavaRDD;

public interface Question {

    void answer(JavaRDD<String> lines);
}
