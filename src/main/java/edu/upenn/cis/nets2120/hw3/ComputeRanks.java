package edu.upenn.cis.nets2120.hw3;

import java.io.IOException;
import static java.lang.Math.*;
import static java.lang.Math.abs;
import java.util.ArrayList;
import java.util.List;
import java.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.SparkConnector;
import scala.Tuple2;

public class ComputeRanks {
	/**
	 * The basic logger
	 */
	static Logger logger = LogManager.getLogger(ComputeRanks.class);

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	
	JavaSparkContext context;
	
	public ComputeRanks() {
		System.setProperty("file.encoding", "UTF-8");
	}

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public void initialize() throws IOException, InterruptedException {
		logger.info("Connecting to Spark...");

		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		logger.debug("Connected!");
	}
	
	/**
	 * Fetch the social network from the S3 path, and create a (followed, follower) edge graph
	 * 
	 * @param filePath
	 * @return JavaPairRDD: (followed: int, follower: int)
	 */
	JavaPairRDD<Integer,Integer> getSocialNetwork(String filePath) {
		JavaRDD<String[]> file = context.textFile(filePath, Config.PARTITIONS) //file array
				.map(line -> line.toString().split(" "));
		JavaPairRDD<Integer, Integer> edgeRDD = file //parse integer into edge rdd
				.mapToPair(x -> new Tuple2<Integer, Integer>(Integer.parseInt(x[0]), Integer.parseInt(x[1])));
		JavaRDD<Integer> listOfFollowers = edgeRDD.map(x -> x._2).distinct();
		JavaRDD<Integer> nodes = edgeRDD.map(x -> x._1).distinct();
		JavaRDD<Integer> totalNodes = listOfFollowers.union(nodes).distinct(); //all total nodes - merge 1 and 2 and use intersection
		System.out.println("This graph contains " + totalNodes.count() + " nodes and " + edgeRDD.count() + " edges");
		
		return edgeRDD.distinct(); //make sure no duplicate edges are returned
	}
	
	private JavaRDD<Integer> getSinks(JavaPairRDD<Integer,Integer> network) {
		System.out.println("Trying to get sinks");
		JavaRDD<Integer> listOfFollowers = network.map(x -> x._2).distinct(); //make all followers
		JavaRDD<Integer> nodes = network.map(x -> x._1).distinct(); //make nodes
		JavaRDD<Integer> totalNodes = listOfFollowers.union(nodes); //all total nodes
		System.out.println("Got RDD of total nodes");
		JavaPairRDD<Integer, Integer> orig = totalNodes //intuitively, we merge all and it has to have indegree or outdegree < 2 to be a sink
				.mapToPair(x -> new Tuple2<Integer, Integer>(x, 1))
				.reduceByKey((a, b) -> a+b)
				.filter(x -> (x._2 < 2));
		JavaRDD<Integer> sinks = orig.map(x -> x._1).intersection(listOfFollowers); //must be a follower if it's a sink
		System.out.println("This graph contains " + sinks.count() + " sinks"); //debug
		return sinks;
	}

	/**
	 * Main functionality in the program: read and process the social network
	 * 
	 * @throws IOException File read, network, and other errors
	 * @throws InterruptedException User presses Ctrl-C
	 */
	
	
	public void run(int dmax, int imax, boolean debugMode) throws IOException, InterruptedException {
		logger.info("Running");

		// Load the social network
		// followed, follower
		JavaPairRDD<Integer, Integer> network = getSocialNetwork(Config.SOCIAL_NET_PATH); //get social network
		JavaRDD<Integer> sinks = getSinks(network); //get sinks
		JavaPairRDD<Integer, Integer> sinksToItself = sinks.mapToPair(x -> new Tuple2<Integer, Integer>(x, x)); //map sinks to itself, will be used later
		JavaPairRDD<Integer, Integer> backLinks = network //create backlinks
				.mapToPair(x -> x.swap())
				.join(sinksToItself)
				.filter(x -> (x._2._2 != x._2._1))
				.mapToPair(x -> new Tuple2<Integer, Integer>(x._2._1, x._2._2))
				.mapToPair(x -> x.swap()); //followed and follower
		
		backLinks.collect().stream().forEach(item -> { //used for debug
			System.out.println("Backlinks:" + item._1 + " " + item._2);
		});
		
		network.collect().stream().forEach(item -> { //used for debug
			System.out.println("Network:" + item._1 + " " + item._2);
		});
		
		JavaPairRDD<Integer, Integer> networkWithBacklinks = network.union(backLinks); //union to add backlinks
		System.out.println("Added " + backLinks.count() + " backlinks");
		
		networkWithBacklinks.collect().stream().forEach(item -> {
			System.out.println("Network with backlinks:" + item._1 + " " + item._2);
		});
		
		JavaPairRDD<Integer, Double> nodeTransferRDD = networkWithBacklinks //network = edgerdd
				.mapToPair(item -> new Tuple2<Integer, Double>(item._1, 1.0))
				.reduceByKey((a, b) -> a + b)
				.mapToPair(item -> new Tuple2<Integer, Double> (item._1, 1.0 / item._2));
				
		JavaPairRDD<Integer, Tuple2<Integer, Double>> edgeTransferRDD = networkWithBacklinks.join(nodeTransferRDD); //as done in lab
		
		edgeTransferRDD.collect().stream().forEach(item -> { //used for debug
			System.out.println(item._1 + ": (" + item._2._1 + ", " + item._2._2 + ")");
		});
		
		System.out.println("Done constructing edgeTransferRDD");
		
		double d = 0.15;
		double delta = Double.MAX_VALUE;
		
		JavaPairRDD<Integer, Double> pageRankRDD = networkWithBacklinks.mapToPair(item -> new Tuple2<Integer, Double>(item._1, 1.0)).distinct();
		System.out.println("Checking page rank RDD");
		pageRankRDD.collect().stream().forEach(item -> {
			System.out.println(item._1 + " : " + item._2);
		});
		//as done in lab
		System.out.println("About to start iterations");
		for (int i = 0; i < 25; i++) {
			System.out.println("Iteration " + i);
			JavaPairRDD<Integer, Double> propagateRDD = edgeTransferRDD
					.join(pageRankRDD)
					.mapToPair(item -> new Tuple2<Integer, Double> (item._2._1._1, item._2._2 * item._2._1._2));
			JavaPairRDD<Integer, Double> pageRankRDD2= propagateRDD
					.reduceByKey((a, b) -> a+b)
					.mapToPair(item -> new Tuple2<Integer, Double>(item._1, (d) + (1-d) * item._2)); //add decay factor
			System.out.println("pageRankRDD Size : " + pageRankRDD.count());
;			pageRankRDD.collect().stream().forEach(item -> {
				System.out.println(item._1 + " : " + item._2);
			});
			JavaPairRDD<Integer, Double> pageRankRDDDifference = pageRankRDD2
					.join(pageRankRDD)
					.mapToPair(item -> new Tuple2<Integer, Double>(item._1, (Math.abs(item._2._1 - item._2._2))));
			delta = pageRankRDDDifference //sort by double, then take the highest delta
					.mapToPair(x -> x.swap())
					.sortByKey(false, Config.PARTITIONS)
					.first()
					._1;
//			pageRankRDDDifference.collect().stream().forEach(item -> {
//				System.out.println(item._1 + ": (" + item._2 + ")");
//			});
			System.out.println("maximum delta = " + delta);
			pageRankRDD = pageRankRDD2; //reset to new iteration
			
			//handle debug mode
			if (debugMode) {
				List<Tuple2<Double, Integer>> allElements = pageRankRDD
						.mapToPair(x -> x.swap())
						.sortByKey(false)
						.collect();
				allElements.forEach(item -> {
					System.out.println(item._2 + " " + item._1);
				});
			}
			if (delta < 0.001) { //check when to terminate
				System.out.println("maximum delta greater than dmax, terminating");
				break;
			}
		}
		
		List<Tuple2<Double, Integer>> topTenElements = pageRankRDD //top ten elements by rank
				.mapToPair(x -> x.swap())
				.sortByKey(false)
				.take(10);
		topTenElements.forEach(item -> {
			System.out.println(item._2 + " " + item._1);
		});
		
		logger.info("*** Finished social network ranking! ***");
	}


	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		logger.info("Shutting down");

		if (spark != null)
			spark.close();
	}
	
	

	public static void main(String[] args) {
		final ComputeRanks cr = new ComputeRanks();
		
		try {
			cr.initialize();
			if (args.length == 1) {
				int dmax = Integer.parseInt(args[0]);
				int imax = 25;
				boolean debugMode = false;
				cr.run(dmax, imax, debugMode);
			} else if (args.length == 2) {
				int dmax = Integer.parseInt(args[0]);
				int imax = Integer.parseInt(args[1]);
				boolean debugMode = false;
				cr.run(dmax, imax, debugMode);
			} else if (args.length == 3) {
				int dmax = Integer.parseInt(args[0]);
				int imax = Integer.parseInt(args[1]);
				boolean debugMode = true;
				cr.run(dmax, imax, debugMode);
			} else {
				int dmax = 30;
				int imax = 25;
				boolean debugMode = false;
				cr.run(dmax, imax, debugMode);
			}
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			cr.shutdown();
		}
	}

}
