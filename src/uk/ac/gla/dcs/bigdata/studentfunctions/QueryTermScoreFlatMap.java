package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentQueryScore;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentTermsScores;

import java.util.*;

/**
 * The QueryTermScoreFlatMap class is responsible for transforming a given DocumentQueryTermsScores object
 * into a list of DocumentQueryScore objects by pairing each query with the document and calculating
 * the overall score of the document for that query.
 */
public class QueryTermScoreFlatMap implements FlatMapFunction<DocumentTermsScores, DocumentQueryScore> {


	private static final long serialVersionUID = 223554196155293068L;
	Broadcast<List<Query>> broadcastQueries;// A broadcast variable containing a list of all queries
	
 /**
 * Constructor for QueryTermScoreFlatMap.
 * @param broadcastQueries A broadcasted list of Query objects.
 */
    public QueryTermScoreFlatMap(Broadcast<List<Query>> broadcastQueries) {
        this.broadcastQueries = broadcastQueries;
    }
/**
 * The call method is invoked on each DocumentQueryTermsScores object in the dataset.
 * @param value The DocumentQueryTermsScores object to be processed.
 * @return An iterator over a collection of DocumentQueryScore objects.
 * @throws Exception If any error occurs during processing.
 */
	@Override
    public Iterator<DocumentQueryScore> call(DocumentTermsScores value) throws Exception {
		List<Query> queries = broadcastQueries.value();  // get quiries list
        List<Double> dphTermScore = value.getScores();  // get DocumnetQueryTermsScores中的List<double> score
        List<String> docTerm = value.getTerms();  // get DocumnetQueryTermsScores中的List<String> terms
        List<DocumentQueryScore> queryScore = new ArrayList<>();
//		get every query from all query
        for (Query query : queries){
        	List<String> queryTerms = query.getQueryTerms();
        	int length = queryTerms.size();  // Get the number of terms in the current query
            double scoreSum = 0.0;
//          get every term from each query
            for (String term : queryTerms) {
            	int index = docTerm.indexOf(term);// Find the index of the term in the document's term list
            	scoreSum = scoreSum + dphTermScore.get(index);// Accumulate the score for this term
            }
            if (scoreSum == 0) { continue; }  // Skip the current query if the score sum is zero
            DocumentQueryScore article = new DocumentQueryScore(value.getId(), value.getTitle(), scoreSum/length, query);// Create a new DocumentQueryScore object with the calculated average score for the query
            queryScore.add(article); // Add the new DocumentQueryScore object to the list
        }
     // Return an iterator over the list of DocumentQueryScore objects
        return queryScore.iterator();
    }
}

