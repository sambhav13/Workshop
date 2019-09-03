package com.app.workshop.intro;

import com.app.workshop.model.Employee;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import scala.collection.Seq;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class SparkApp {

    public static void main(String[] args) {


       SparkSession spark =  SparkSession.builder()
                            .master("local[*]")
                            .appName("Firs Spark App")
                            .getOrCreate();

       //Reading a Sequence/ collection

        List<Integer> integers = Arrays.asList(1, 2, 4, 5);
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        JavaRDD<Integer> rdd = javaSparkContext.parallelize(integers);
        long totalCount = rdd.count();
        System.out.println(totalCount);


        //Converting RDD to DataFrame to Dataset

        Employee e1 =  new Employee(1,"aaron",29,"finance");
        Employee e2 =  new Employee(2,"rick",32,"sales");
        JavaRDD<Employee> rdd_emp = javaSparkContext.parallelize(Arrays.asList(e1,e2));

        //Dataframe creation
        Dataset<Row> empDF = spark.createDataFrame(rdd_emp, Employee.class);
        empDF.show(false);

        //Generating encoders for spark
        Encoder<Employee> employee_encoder = Encoders.bean(Employee.class);

        //Converting to Dataset
        Dataset<Employee> empDS = empDF.as(employee_encoder);
        empDS.show(false);

        //applying map transformation to a Dataset
        Dataset<String> departMentDS = empDS.map((MapFunction<Employee, String>) p -> {
            String d = p.getDepartment();
            return d;
        }, Encoders.STRING());
        departMentDS.show(false);


        // Define schema for the Dataframe
        StructType employeeSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("department", StringType, false, Metadata.empty())
        });

        Row r1 =  RowFactory.create(1,"aaron",29,"finance");
        Row r2 =  RowFactory.create(2,"peterson",33,"sales");

        Dataset<Row> empDF_fromRows = spark.createDataFrame(Arrays.asList(r1, r2), employeeSchema);
        empDF_fromRows.printSchema();
        empDF_fromRows.show(false);

        spark.stop();
        System.exit(0);

        //Reading from a csv file
        Dataset<Row> employeeDF =  spark.read()
                .option("header","true")
                .csv("src/main/resources/Employee.csv");
        employeeDF.printSchema();
        employeeDF.show(false);


        //Infereing the schema
        Dataset<Row> employeeDF_withSchema =  spark.read()
                .option("header","true")
                .option("inferSchema","true")
                .csv("src/main/resources/Employee.csv");
        employeeDF_withSchema.printSchema();
        employeeDF_withSchema.show(false);







    }
}
