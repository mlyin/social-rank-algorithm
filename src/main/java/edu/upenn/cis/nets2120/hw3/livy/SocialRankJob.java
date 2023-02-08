package edu.upenn.cis.nets2120.hw3.livy;

import java.io.IOException;
import java.util.*;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.SparkConnector;
import scala.Tuple2;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class SocialRankJob implements Job<List<MyPair<Integer,Double>>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	
	JavaSparkContext context;

	private boolean useBacklinks;

	private String source;
	

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 * @throws DynamoDbException 
	 */
	public void initialize() throws IOException, InterruptedException {
		System.out.println("Connecting to Spark...");
		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		System.out.println("Connected!");
	}
	
	/**
	 * Fetch the social network from the S3 path, and create a (followed, follower) edge graph
	 * 
	 * @param filePath
	 * @return JavaPairRDD: (followed: int, follower: int)
	 */
	JavaPairRDD<Integer,Integer> getSocialNetwork(String filePath) {
		JavaRDD<String[]> file = context.textFile(filePath, Config.PARTITIONS)
				.map(line -> line.toString().split(" "));
		JavaPairRDD<Integer, Integer> edgeRDD = file
				.mapToPair(x -> new Tuple2<Integer, Integer>(Integer.parseInt(x[0]), Integer.parseInt(x[1])));
		JavaRDD<Integer> listOfFollowers = edgeRDD.map(x -> x._2).distinct();
		JavaRDD<Integer> nodes = edgeRDD.map(x -> x._1).distinct();
		JavaRDD<Integer> totalNodes = listOfFollowers.union(nodes).distinct();
		System.out.println("This graph contains " + totalNodes.count() + " nodes and " + edgeRDD.count() + " edges");
		return edgeRDD;
	}
	
	private JavaRDD<Integer> getSinks(JavaPairRDD<Integer,Integer> network) {
		System.out.println("Trying to get sinks");
		JavaRDD<Integer> listOfFollowers = network.map(x -> x._2).distinct();
		JavaRDD<Integer> nodes = network.map(x -> x._1).distinct();
		JavaRDD<Integer> totalNodes = listOfFollowers.union(nodes);
		System.out.println("Got RDD of total nodes");
		JavaPairRDD<Integer, Integer> orig = totalNodes
				.mapToPair(x -> new Tuple2<Integer, Integer>(x, 1))
				.reduceByKey((a, b) -> a+b)
				.filter(x -> (x._2 < 2));
		JavaRDD<Integer> sinks = orig.map(x -> x._1).intersection(listOfFollowers);
		System.out.println("This graph contains " + sinks.count() + " sinks");
		return sinks;
	}

	/**
	 * Main functionality in the program: read and process the social network
	 * 
	 * @throws IOException File read, network, and other errors
	 * @throws DynamoDbException DynamoDB is unhappy with something
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public List<MyPair<Integer,Double>> run() throws IOException, InterruptedException {
		int dmax = 30;
		System.out.println("Running");

		// Load the social network
		// followed, follower
		JavaPairRDD<Integer, Integer> network = getSocialNetwork(this.source); //get social network
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
		if (useBacklinks) {
			network = network.union(backLinks); //union to add backlinks
			System.out.println("Added " + backLinks.count() + " backlinks");
			
			network.collect().stream().forEach(item -> {
				System.out.println("Network with backlinks:" + item._1 + " " + item._2);
			});
		} //else backlinks not created
		
		JavaPairRDD<Integer, Double> nodeTransferRDD = network //network = edgerdd
				.mapToPair(item -> new Tuple2<Integer, Double>(item._1, 1.0))
				.reduceByKey((a, b) -> a + b)
				.mapToPair(item -> new Tuple2<Integer, Double> (item._1, 1.0 / item._2));
				
		JavaPairRDD<Integer, Tuple2<Integer, Double>> edgeTransferRDD = network.join(nodeTransferRDD); //as done in lab
		
		edgeTransferRDD.collect().stream().forEach(item -> { //used for debug
			System.out.println(item._1 + ": (" + item._2._1 + ", " + item._2._2 + ")");
		});
		
		System.out.println("Done constructing edgeTransferRDD");
		
		double d = 0.15;
		double delta = Double.MAX_VALUE;
		
		JavaPairRDD<Integer, Double> pageRankRDD = network.mapToPair(item -> new Tuple2<Integer, Double>(item._1, 1.0)).distinct();
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
			pageRankRDD.collect().stream().forEach(item -> {
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
//					pageRankRDDDifference.collect().stream().forEach(item -> {
//						System.out.println(item._1 + ": (" + item._2 + ")");
//					});
			System.out.println("maximum delta = " + delta);
			pageRankRDD = pageRankRDD2; //reset to new iteration
			
			//handle debug mode
			if (delta < dmax) {
				System.out.println("maximum delta greater than dmax (2), terminating");
				break;
			}
		}
//		.sortByKey(false)
//		.take(10);
		
		List<Tuple2<Double, Integer>> topTenElements = pageRankRDD
				.mapToPair(x -> x.swap())
				.sortByKey(false)
				.take(10);
		topTenElements.forEach(item -> {
			System.out.println(item._2 + " " + item._1);
		});
		
		List<MyPair<Integer, Double>> myPairList = new ArrayList<MyPair<Integer, Double>>();
		
		for (int i = 0; i < topTenElements.size(); i++) {
			Tuple2<Double, Integer> currentTuple = topTenElements.get(i);
			MyPair<Integer, Double> curr = new MyPair<Integer, Double>(currentTuple._2, currentTuple._1);
			myPairList.add(curr);
		}	
			
		System.out.println("*** Finished social network ranking! ***");
		return myPairList;

	}

	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		System.out.println("Shutting down");
	}
	
	public SocialRankJob(boolean useBacklinks, String source) {
		System.setProperty("file.encoding", "UTF-8");
		
		this.useBacklinks = useBacklinks;
		this.source = source;
	}

	@Override
	public List<MyPair<Integer,Double>> call(JobContext arg0) throws Exception {
		initialize();
		return run();
	}

}
