package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class VentesParAnnee {
    public static void main(String[] args) {
        String year = "2021";
        SparkConf sparkConf = new SparkConf()
                .setAppName("total des ventes des villes par annee")
                .setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rddVenteLine = sparkContext.textFile("src/main/ventes.txt");
        JavaRDD<String> rddVenteAnnee = rddVenteLine.filter(vente -> vente.split(" ")[0].endsWith(year));
        JavaPairRDD<String, Double> rddPairVentesVille = rddVenteAnnee.mapToPair(vente->{
            String[] token = vente.split(" ");
            return new Tuple2<>(token[1], Double.parseDouble(token[3]));
        });
        JavaPairRDD<String, Double> rddTuple = rddPairVentesVille.reduceByKey((a,b) -> a+b);
        List<Tuple2<String, Double>> totalVentes  = rddTuple.collect();

        System.out.println("total des ventes par ville Annee: "+ year);
        totalVentes.forEach(tuple-> System.out.println(tuple._1()+" "+tuple._2()));
    }
}
