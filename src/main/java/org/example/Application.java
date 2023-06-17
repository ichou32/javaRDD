package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Application {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("spark RDD")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<Double> rdd1 = sc.parallelize(Arrays.asList(12.0, 9.0, 14.25, 18.00, 15.50, 16.75, 8.75));
        JavaRDD<Double> rdd2 = rdd1.map(note -> note+1);
        JavaRDD<Double> rdd3 = rdd2.filter(note -> note>=10);
        List<Double> list= rdd3.collect();
        for (Double note:list){
            System.out.println(note);
        }

    }
}