package com.app.workshop.sql;

import com.app.workshop.model.Student;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class SparkSql {

    public static void main(String[] args) {
        SparkSession spark =  SparkSession.builder()
                //.master("local[*]")
                .appName("SparkSQl Spark App")
                .config("spark.eventLog.enabled","true")
                .getOrCreate();


        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());

        SQLContext sqlContext = spark.sqlContext();
        Student s1 = new Student(1,"rick");
        Student s2 = new Student(2,"aaron");


        JavaRDD<Student> studentRdd = javaSparkContext.parallelize(Arrays.asList(s1,s2));



        Dataset<Row> intDF = spark.createDataFrame(studentRdd, Student.class);
        intDF.printSchema();
        intDF.show(false);


        intDF.registerTempTable("student");
        spark.sql("select name from student").show(false);
    }
}
