package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class VentesParVille {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("ventes par ville")
                .setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rddVentesLine = sparkContext.textFile("src/main/ventes.txt");
        JavaPairRDD<String, Double> rddPairVentes = rddVentesLine.mapToPair(vente -> {
            String[] venteRow = vente.split(" ");
            String ville = venteRow[1];
            Double prix = Double.parseDouble(venteRow[3]);
            return new Tuple2<>(ville, prix);
        });
        JavaPairRDD<String, Double> rddTotalVentes = rddPairVentes.reduceByKey((a,b) -> a+b);
        List<Tuple2<String, Double>> rddVenteVille = rddTotalVentes.collect();
        for (Tuple2<String, Double> tuple: rddVenteVille){
            System.out.println(tuple._1()+ " "+tuple._2());
        }
    }
}
