package uk.ac.gla.dcs.bigdata.providedfunctions;

import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class QueryFormaterMap implements MapFunction<Row,Query> {

	private static final long serialVersionUID = 6475166483071609772L;

	// Declares a field processor of type TextPreProcessor.
	// The  transient keyword indicates that this field should not be serialised, usually because it may contain non-serialisable objects or to save memory/disk space.
	// Instances of the TextPreProcessor class may be used for text processing, such as word splitting, deactivation, etc.
	private transient TextPreProcessor processor;
	
	@Override
	public Query call(Row value) throws Exception {
	
		// Avoid duplicate object creation
		if (processor==null) processor = new TextPreProcessor();
		
		// Extract the string from the input Row object, where it is assumed that the Row object contains only one field, the original query string.
		String originalQuery = value.mkString();
		
		// Use processor to process the raw query string for text pre-processing: word splitting, stop word filtering, stemming extraction.
		// Converts each row in the query list into a list of terms.
		List<String> queryTerms = processor.process(originalQuery);
		
		// Create a short array to store the count of each term
		short[] queryTermCounts = new short[queryTerms.size()];
		// Loop, initialised with each element set to 1. It is assumed that each term occurs only once in the query.
		for (int i =0; i<queryTerms.size(); i++) queryTermCounts[i] = (short)1;
		
		
//		// Number of occurrences of statistical terms
//		Map<String, Short> termCountsMap = new HashMap<>();
//		for (String term : queryTerms) {
//			 // Checks if the current traversed term is already contained in the termCountsMap map.
//			 // The containsKey method returns a boolean value, true if the map contains the specified key.
//		    if (termCountsMap.containsKey(term)) {
//		    	// The put method updates the count value of the term in the mapping
//		        termCountsMap.put(term, (short)(termCountsMap.get(term) + 1));
//		    } else {
//		        termCountsMap.put(term, (short)1);
//		    }
//		}
//
//		// Updating queryTerms and queryTermCounts
//		List<String> queryTerms1 = new ArrayList<>(termCountsMap.keySet());
//		short[] queryTermCounts1 = new short[queryTerms1.size()];
//
//		for (int i = 0; i < queryTerms1.size(); i++) {
//		    String term = queryTerms1.get(i);
//		    queryTermCounts1[i] = termCountsMap.get(term);
//		}
		
		
		// Load the obtained data with the Query object.
		Query query = new Query(originalQuery, queryTerms, queryTermCounts);
		// Query query = new Query(originalQuery, queryTerms1, queryTermCounts1);
		
		return query;
	}

}
