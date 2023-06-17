package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Application2 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Count words")
                .setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rddLines = sparkContext.textFile("src/main/words.txt");
        JavaRDD<String> rddWords = rddLines.flatMap(line ->{
            String[] words = line.split(" ");
            return Arrays.asList(words).iterator();
        });
        JavaPairRDD<String, Integer> pairRDD = rddWords.mapToPair(word ->new Tuple2<>(word, 1));
        List<Tuple2<String, Integer>> wordCout = pairRDD.reduceByKey((a, b) -> a+b).collect();
        for (Tuple2<String, Integer> tuple: wordCout){
            System.out.println(tuple._1() +" "+ tuple._2());
        }
    }
}
