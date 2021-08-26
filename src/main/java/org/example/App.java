package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws InterruptedException {



        SparkConf conf = new SparkConf().setAppName("razor-app").setMaster("local[*]");

        JavaSparkContext context = new JavaSparkContext(conf);

        List<Integer> integerList = Arrays.asList(1,2,3,4,5,6,7,8,8,9,9,6,5,3,2,4,5);

        JavaRDD javaRDD = context.parallelize(integerList);

        javaRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer o) throws Exception {
                System.out.println("rrd: " + o);
            }
        });

        javaRDD.foreach((VoidFunction<Integer>) o -> {

            System.out.println("rrd: " + o);
            Thread.sleep(3000);
        }  );

        Thread.sleep(1000000);
        context.stop();

    }
}
