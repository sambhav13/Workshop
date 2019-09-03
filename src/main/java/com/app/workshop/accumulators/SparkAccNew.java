package com.app.workshop.accumulators;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;

import java.io.Serializable;
import java.util.Arrays;


class MyComplex implements Serializable {

     int x;
     int y;

    public MyComplex(int x, int y) {
        this.x = x;
        this.y = y;

    }

    public void reset() {
        x = 0;
        y = 0;
    }

    public MyComplex add(MyComplex p) {
        x = x + p.x;
        y = y + p.y;
        return this;
    }
}
//Accumulator class
class CustomAccumulator extends AccumulatorV2<MyComplex, MyComplex> implements Serializable {

    private MyComplex myc= new MyComplex(0,0);


    @Override
    public boolean isZero() {
        return (this.myc.x == 0 && myc.y == 0);
    }

    @Override
    public AccumulatorV2<MyComplex, MyComplex> copy() {
        return new CustomAccumulator();
    }

    @Override
    public void reset() {
        myc.reset();
    }

    @Override
    public void add( MyComplex v) {
        myc.add(v);
    }

    @Override
    public void merge( AccumulatorV2<MyComplex, MyComplex> other) {
        myc.add(other.value());
    }

    @Override
    public MyComplex value() {
        return myc ;
    }
}

//        def reset(): Unit = {
//        validationAccumulators = new SampleOutputStat()
//        }
//
//        def add(input: String): Unit = {
//        validationAccumulators = validationAccumulators.add(input)
//        }
//
//        def value: SampleOutputStat = {
//        validationAccumulators
//        }
//
//        def isZero: Boolean = {
//        validationAccumulators.isEmpty
//        }
//
//        def copy(): CustomAccumulator = {
//        new CustomAccumulator(validationAccumulators)
//        }
//
//        publi merge(other: AccumulatorV2[String, SampleOutputStat]) = {
//        validationAccumulators = validationAccumulators.merge(other.value)
//        }
//
//        }
public class SparkAccNew  implements Serializable{

    public static void main(String[] args) {


        SparkSession spark =  SparkSession.builder()
                .master("local[*]")
                .appName("Firs Spark App")
                .getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        CustomAccumulator customAccumulator = new CustomAccumulator();
        spark.sparkContext().register(customAccumulator, "addition");



        JavaRDD<Integer> rdd = javaSparkContext.parallelize(Arrays.asList(1, 2));
         rdd.foreach( (VoidFunction<Integer>) x -> {
              customAccumulator.add(new MyComplex(x,x));
         });


        System.out.println("the value of customaccumualtor  x is: "+customAccumulator.value().x);
        System.out.println("the value of customaccumualtor  y is: "+customAccumulator.value().y);

    }
}



