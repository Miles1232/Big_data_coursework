package uk.ac.gla.dcs.bigdata.providedfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/**
 * Converts a Row containing a String Json news article into a NewsArticle object 
 * @author Richard
 *
 */
public class NewsFormaterMap implements MapFunction<Row,NewsArticle> {
	// Defines the NewsFormaterMap class, which implements Spark's MapFunction interface.
	// This interface requires the implementation of a call method that converts the input Row object to a NewsArticle object.

	// The sequence version UID required by the Java serialisation mechanism to ensure class version consistency during deserialisation.
	private static final long serialVersionUID = -4631167868446468097L;

	// Declares an instance of ObjectMapper for use with JSON data.
	// The transient keyword indicates that this field will not be serialised, this is because the ObjectMapper instance does not need to be serialised for transmission with the rest of the class.
	private transient ObjectMapper jsonMapper;
	
	@Override
	// Definition of the call method, which is part of the MapFunction interface. This method takes a Row object as an argument and returns a NewsArticle object.
	// The throws Exception indicates that this method may throw an exception.
	public NewsArticle call(Row value) throws Exception {

		// Conditional checking ensures that jsonMapper is instantiated. This is a pattern of inert initialisation, where a new ObjectMapper instance is created only if jsonMapper is null, i.e. not yet initialised.
				// This avoids unnecessary object creation, especially if this MapFunction is likely to be called a lot in parallel.
		if (jsonMapper==null) jsonMapper = new ObjectMapper();
		
		// The core of the conversion operation.
		// It calls the jsonMapper's readValue method, which parses the string converted from the Row object (assumed to be in JSON format) into an instance of the NewsArticle class
		// The value.mkString() method converts the Row object to a string, and NewsArticle.class tells ObjectMapper what type of object to convert the JSON data to.
		NewsArticle article = jsonMapper.readValue(value.mkString(), NewsArticle.class);
		
		return article;
	}
		
		
	
}
