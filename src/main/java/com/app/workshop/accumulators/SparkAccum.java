package com.app.workshop.accumulators;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;


public class SparkAccum {

    public static void main(String[] args) {


        SparkSession spark =  SparkSession.builder()
              .master("local[*]")
                .appName("Firs Spark App")
                .getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        final Accumulator<Double> acc =  javaSparkContext.doubleAccumulator(0.0);




        javaSparkContext.parallelize(Arrays.asList(1.1, 2.2, 3.2))
               .foreach(x -> acc.add( x));


        System.out.println("print value of current accumulator:"+acc.value());


        Accumulator<Integer> accumulator = javaSparkContext.accumulator(0);
        javaSparkContext.parallelize(Arrays.asList(1.1, 2.2, 3.2))
                .foreach(x -> acc.add( x));
        spark.stop();
    }
}
