package com.app.workshop.udf;

import com.app.workshop.model.Student;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.api.java.UDF1;

import java.util.Arrays;

public class SparkUdf {
    public static void main(String[] args) {


        SparkSession spark =  SparkSession.builder()
                .master("local[*]")
                .appName("Firs Spark App")
                .getOrCreate();


        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        SQLContext sqlContext = spark.sqlContext();
        sqlContext.udf().register("upperCase",(UDF1<String,String>) p->{

            return p.toUpperCase();
        }, DataTypes.StringType);

        Student s1 = new Student(1,"rick");
        Student s2 = new Student(2,"aaron");
        JavaRDD<Student> studentRdd = javaSparkContext.parallelize(Arrays.asList(s1,s2));


        Dataset<Row> intDF = spark.createDataFrame(studentRdd, Student.class);
        intDF.printSchema();
        intDF.show(false);
        intDF.registerTempTable("student");
        spark.sql("select name from student").show(false);

        spark.sql("select upperCase(name) as NAME from student").show(false);

    }
}
