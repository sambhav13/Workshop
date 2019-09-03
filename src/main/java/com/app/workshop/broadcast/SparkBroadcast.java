package com.app.workshop.broadcast;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class SparkBroadcast {
    public static void main(String[] args) {

        SparkSession spark =  SparkSession.builder()
                .master("local[*]")
                .appName("Firs Spark App")
                .getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        Broadcast<Integer> brVal = javaSparkContext.broadcast(10);


        JavaRDD<Integer> rdd = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4));

        JavaRDD<Integer> mappedRdd = rdd.map((Function<Integer, Integer>) p -> {

            return p.intValue() + brVal.getValue();
        });

        mappedRdd.collect().forEach(p -> {

            System.out.println("value: "+p);
        });

        spark.stop();

    }
}
