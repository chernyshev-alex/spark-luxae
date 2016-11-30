package com.acme.luxae;

import com.google.common.base.Optional;
import java.time.LocalDate;
import java.util.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple4;

public class SparkApp {
    
   public final String appName;

   // map month to index of the year
   static final Map<String, Integer> months =  Collections.unmodifiableMap(new HashMap<String, Integer>() {
        { 
            put("JUN", 1); put("JAN", 1); put("FEB", 2); put("MAR", 3);
            put("APR", 4); put("MAY", 5); put("JUB", 6); put("JUL", 6); put("AUG", 7); put("SEP", 9);
            put("OCT", 10); put("NOV", 11); put("DEC", 12);
        }});
   
    public SparkApp(String __appName) {
        this.appName = __appName;
    }
    /**
     * Run calculation in distributed spark cluster.
     * @param sourceFile - quotes from example_input.txt
     * @param multipFile  - data from INSTRUMENT_PRICE_MODIFIER.csv uploaed from database 
     * @return all calculations from cluster
     */
    public Map runWithData(String sourceFile, String multipFile) {
        // !!!  change to : 
        // local[N/*] - will take N CPU cores or all available (vertical scalability)
        // master - "spark://master:7077" to run on a Spark standalone cluster (horizontal scalability)
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // load source file to cluster and make RDD
        JavaRDD<String> sourceRdd = sc.textFile(sourceFile);
        JavaPairRDD<String, Tuple2<LocalDate, Double>> quotes = sourceRdd.mapToPair(lineParserFun());
       
        // upload from db to hdfs://INSTRUMENT_PRICE_MODIFIER.csv  and  make RDD INSTRUMENT_PRICE_MODIFIER  
        //JavaPairRDD<String, Double> mult = sc.parallelize(Arrays.asList(multip))
        //        .mapToPair((a) -> new Tuple2(a._1, a._2));

        JavaRDD<String> multRdd = sc.textFile(multipFile);
        JavaPairRDD<String, Double> mult = multRdd.mapToPair(multParserFun());
        
        
        // do quotes left outer join with mult
        JavaPairRDD<String, Tuple2<Tuple2<LocalDate, Double>, Optional<Double>>> joined = 
                quotes.leftOuterJoin(mult);
        joined.cache();  // cache this RDD in cluster

        // TASK1. calculate mean, min, max for ticker INSTRUMENT1 for all period 
       List<Tuple4> meanIns1 = calculateMean(joined.filter((t) -> t._1.equalsIgnoreCase("INSTRUMENT1")))
               .collect();

        // TASK2. calculate mean, min, max for ticker INSTRUMENT2 for November 2014
       List<Tuple4> meanIns2_11_2014 = calculateMean(
               joined.filter((t) -> t._1.equalsIgnoreCase("INSTRUMENT2") && isInPeriod(t._2._1._1,  11, 2014)))
               .collect();

        // TASK10. calculate mean, min, max for all tickers
       List<Tuple4> meanAll = calculateMean(joined)
               .collect();
       
       sc.stop();  

       // collect all results
       Map result = new HashMap(); 
       result.put("MEAN_INSTRUMENT1", meanIns1);
       result.put("MEAN_INSTRUMENT2_11_2014", meanIns2_11_2014);
       result.put("MEAN_ALL", meanAll);
       return result;
    }
    
    // All functions below will be serialized and delivered to cluster
    
    /**
     * Calculate mean, max, min for tickers
     * @param rdd - source
     * @return list of tuples with metrics
     */
    static final JavaRDD<Tuple4> 
        calculateMean(JavaPairRDD<String, Tuple2<Tuple2<LocalDate, Double>, Optional<Double>>> rdd) {
        
        JavaPairRDD<String, Tuple4<Double, Integer, Double, Double>> rd2 = rdd.combineByKey(
               // step 1. (price * M, 1, price_min, price_max)
               (t) ->  {
                  Double price = t._1._2 * t._2.or(1.0); // price * (M from database or 1.0 when absent X)
                  return new Tuple4(price, 1, price, price);
               }, 
                // step 2. (sum(price), sum(counter), min, max)
                //  t = (price*M, sum(counter), price*M, price*M)
                //  v = ((data, price), options(M))
               (t, v) -> { 
                   Double price = v._1._2 * v._2.or(1.0); 
                   return new Tuple4(t._1() + price,               // sum(price)
                                    1 + t._2(),                        // inc(counter) 
                                    Double.min(t._3(), price),    // min(_, price)
                                    Double.max(t._4(), price));   // max(_, price)
                    },   
                // step 3. combine results from step 2
               (t, v) -> new Tuple4(t._1() + v._1(), t._2() + v._2(), t._3(), t._4())
        );        

       // return (ticker, mean = totals_price / totals_counter, min, max)
       return rd2.map(
               (t) -> new Tuple4(t._1,  // ticker
                                 t._2._1() / t._2._2(), // mean 
                                 t._2._3(), t._2._4())  // min, max
       );
    }
    
    /**
     * @return true in date = month and year
     */
    static final boolean isInPeriod(LocalDate ld, int m, int y) {
           return (ld.getMonthValue() == m) && (ld.getYear() == y);
    };

    /**
     * Parse data/INSTRUMENT_PRICE_MODIFIER.csv lines
     * @return (ticker, multiplier)
     */
    PairFunction multParserFun() {
        return (PairFunction) (Object s) -> {
            String[] parts = ((String) s).split(",");
            return new Tuple2(parts[0], Double.parseDouble(parts[1]));
        };
    };
        
    /**
     * Parse data/example_input.txt
     * @returns fun (a, _) => Tuple2(ticker, Tuple2(date, price))
     */
    PairFunction lineParserFun() {
        // TODO: check null and parse errors
        return (PairFunction) (Object quote) -> {
            String[] parts = ((String)quote).split(",");
            String[] date_parts = parts[1].split("-"); 
            // there is an invalid data  1 Jun
            try {
                LocalDate ld = LocalDate.of(Integer.parseInt(date_parts[2]), 
                    months.getOrDefault(date_parts[1].toUpperCase(), -1),
                    Integer.parseInt(date_parts[0]));
                return new Tuple2(parts[0], new Tuple2(ld, Double.parseDouble(parts[2])));
                
            } catch (java.time.DateTimeException e) {
                // TODO !!!  skip invalid date
                // log.warn()
                // spark.broadcast()
                // return fake data
                return new Tuple2("DATE_PARSE_ERROR", new Tuple2(LocalDate.now(), 0.0));
            }
        };
    }
}
