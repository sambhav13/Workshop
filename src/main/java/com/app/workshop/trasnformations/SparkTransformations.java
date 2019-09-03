package com.app.workshop.trasnformations;

import com.app.workshop.model.Employee;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class SparkTransformations {

    public static void main(String[] args) {


        SparkSession spark =  SparkSession.builder()
                .master("local[*]")
                .appName("Firs Spark App")
                .getOrCreate();


        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        JavaRDD<Integer> integerRDD = javaSparkContext.parallelize(Arrays.asList(1, 3, 4, 5, 6));
        JavaRDD<String> strRdd = integerRDD.map((Function<Integer, String>) p -> {

            return String.valueOf(p);
        });

        System.out.println("Number of partitions:"+strRdd.getNumPartitions());
        System.out.println("Storage Level:"+strRdd.getStorageLevel());
        strRdd.cache();
        strRdd.count();
        strRdd.collect().forEach( p ->{

            System.out.println("value:"+p);
        });


        System.out.println("Total Nunber of elements"+strRdd.count());





    }
}
