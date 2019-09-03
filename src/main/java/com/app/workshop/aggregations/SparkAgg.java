package com.app.workshop.aggregations;

import com.app.workshop.model.Employee;
import com.app.workshop.model.Student;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.Arrays;

public class SparkAgg {

        public static void main(String[] args) {


            SparkSession spark = SparkSession.builder()
                    //.master("local[*]")
                    .appName("SparkAgg Spark App")
                    .config("spark.eventLog.enabled","true")
                    .getOrCreate();

            JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
            JavaRDD<Integer> rdd = javaSparkContext.parallelize(Arrays.asList(1,1,1,2,2,2,3,3));

            //Converting to paired RDD
            JavaPairRDD<Integer, Integer> pairedRdd = rdd.mapToPair((PairFunction<Integer, Integer, Integer>) p -> {

                return new Tuple2<Integer, Integer>(p , p);
            });

            JavaPairRDD<Integer, Integer> reducedRdd = pairedRdd.reduceByKey((v1, v2) -> {
                return v1 + v2;
            });

            reducedRdd.collect().forEach( p ->{
                System.out.println("Key :"+p._1 +" , Value :"+p._2);
            });




            //Creating DataFrame and then aggregating


            Employee e1 =  new Employee(1,"aaron",29,"finance");
            Employee e2 =  new Employee(2,"rick",32,"sales");
            Employee e3 =  new Employee(3,"peterson",29,"finance");
            Employee e4 =  new Employee(4,"shawn",32,"sales");
            JavaRDD<Employee> employeeRdd = javaSparkContext.parallelize(Arrays.asList(e1,e2,e3,e4));


            Dataset<Row> empDF = spark.createDataFrame(employeeRdd, Employee.class);
            empDF.printSchema();
            empDF.show(false);
            empDF.registerTempTable("employee");
            spark.sql("select * from employee").show(false);



            //Aggregations using Spark Sql:
            spark.sql("select department,sum(age) as totalAge from employee group by department").show(false);


            //Aggregations using DataSet/DataFrame APi

            Dataset<Row> aggDF = empDF.groupBy(empDF.col("department")).agg(functions$.MODULE$.sum(empDF.col("age")).name("totalAgeDF"));
            aggDF.printSchema();
            aggDF.show(false);


            spark.stop();


        }
}
