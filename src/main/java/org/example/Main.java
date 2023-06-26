package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {


        List<Double> doubleList = new ArrayList<>();
        doubleList.add(10.11);
        doubleList.add(12.45);
        doubleList.add(13.134);
        doubleList.add(20.32);
        Logger logger = Logger.getLogger(Main.class);
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]")
                ;

        JavaSparkContext context = new JavaSparkContext(conf);

        context.setLogLevel("WARN");
        JavaRDD<Double>  myRDD = context.parallelize(doubleList);



        Double aggregate = myRDD.reduce((aDouble, aDouble2) -> aDouble + aDouble2);
        logger.info(" ########  Aggregate value is" +  aggregate);

        myRDD.map(aDouble -> " Initial Values are " + aDouble)
                .foreach(s -> System.out.println(s))
        ;


        System.out.println(" Total Count is : " +  myRDD.count());

        long count = myRDD.map(aDouble -> 1)
                        .reduce((integer, integer2) -> integer + integer2)
                ;

        System.out.println(" Total count by reduce si " + count);
        context.close();

        System.out.println("Hello world!");
    }
}