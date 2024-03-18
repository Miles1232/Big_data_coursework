package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.util.LongAccumulator;
import static org.apache.spark.sql.functions.*;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.*;
import uk.ac.gla.dcs.bigdata.studentstructures.*;

/**
 * This is the main class where your Spark topology should be specified.
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {


	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it

		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors

		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);

		// Create the spark session
		SparkSession spark = SparkSession
				.builder()
				.config(conf)
				.getOrCreate();


		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries

		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		
		
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

		// Close the spark session
		spark.close();

		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {

			// We have set of output rankings, lets write to disk

			// Create a new folder
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();

			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}

		long endTime = System.currentTimeMillis();
		long duration = endTime - startTime;
		System.out.println("programme runtime: " + duration + " ms");
	}


	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {

		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle

		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------

		//Transform NewsArticle to PreProcessedDocument
		Dataset<PreProcessedDocument> preProcessedDocument = news.map(new PreProcessedDocumentMap(), Encoders.bean(PreProcessedDocument.class));

		//Get all terms in all queries
		Set<String> queryTerms = new HashSet<>(queries.flatMap(new QueryToTermsFlatMap(), Encoders.STRING()).collectAsList());
		Broadcast<Set<String>> broadcastQueryTerms = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryTerms);

		//The total number of documents in the corpus
		LongAccumulator totalDocsInCorpusAccumulator = spark.sparkContext().longAccumulator();
		//The total length of all documents
		LongAccumulator totalDocumentLengthAccumulator = spark.sparkContext().longAccumulator();
		//The map of the sum of different term frequencies for the term across all documents
		Map<String, LongAccumulator> totalTermFrequencyInCorpusMap = new HashMap<>();
		for(String queryTerm : queryTerms){
			totalTermFrequencyInCorpusMap.put(queryTerm, spark.sparkContext().longAccumulator());
		}

		//combine documents and query terms, count statistics in parallel
		Dataset<DocumentTermsStatistics> documentTermsStatistics = preProcessedDocument.map(new DocumentTermsStatisticsMap(
				totalDocumentLengthAccumulator, broadcastQueryTerms, totalDocsInCorpusAccumulator, totalTermFrequencyInCorpusMap),
				Encoders.bean(DocumentTermsStatistics.class));

		//Actions to trigger above map for counting statistics
		documentTermsStatistics.count();

		//The total number of documents in the corpus
		long totalDocsInCorpus = totalDocsInCorpusAccumulator.value();
		//The total length of all documents
		long totalDocumentLength = totalDocumentLengthAccumulator.value();
		//The average document length in the corpus (in terms)
		double averageDocumentLengthInCorpus = (double) totalDocumentLength / totalDocsInCorpus;

		//calculate DPH scores for all <document, term> pairs
		Dataset<DocumentTermsScores> documentTermsScores = documentTermsStatistics.map(new DocumentTermsScoresMap(
				totalDocsInCorpus, averageDocumentLengthInCorpus, totalTermFrequencyInCorpusMap),
				Encoders.bean(DocumentTermsScores.class));

		//Get all queries
		List<Query> queryList = queries.collectAsList();
		Broadcast<List<Query>> broadcastQueries = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryList);

		//Calculate DPH score for every <document, query> pair
		Dataset<DocumentQueryScore> documentQueryScore = documentTermsScores.flatMap(new QueryTermScoreFlatMap(
				broadcastQueries), Encoders.bean(DocumentQueryScore.class)).filter(column("title").isNotNull());

		//aggregated statistics
		Dataset<Row> groupedQuery = documentQueryScore.groupBy(column("query")).agg(
				collect_list(column("docid")).alias("docids"),
				collect_list(column("title")).alias("titles"),
				collect_list(column("queryScore")).alias("queryScores"));
		
		//Remove redundancy and calculate top 10
		Dataset<DocumentRanking> documentRanking = groupedQuery.map(new DocumentRankingMap(),Encoders.bean(DocumentRanking.class));

		return documentRanking.collectAsList(); // replace this with the list of DocumentRanking output by your topology
	}
	}
