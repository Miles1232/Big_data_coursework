package uk.ac.gla.dcs.bigdata.providedutilities;

import java.util.ArrayList;
import java.util.List;

import org.terrier.indexing.tokenisation.Tokeniser;
import org.terrier.terms.BaseTermPipelineAccessor;

/**
 * This class provides pre-processing for text strings based on the Terrier IR platform.
 * In particular, it provides an out-of-the-box function for performing stopword removal
 * and stemming on a text string.
 * 
 * @author Richard
 *
 */
public class TextPreProcessor {

	
	BaseTermPipelineAccessor termProcessingPipeline; // processes an individual term
	Tokeniser tokeniser; // splits a string into multiple terms 
	
	/**
	 * Default Constructor
	 */
	public TextPreProcessor() {
		
		// Each lexical item will be processed by first filtering the deactivated words and then applying the Porter stemming extraction algorithm.
		termProcessingPipeline = new BaseTermPipelineAccessor("Stopwords","PorterStemmer");
		// Splitting a string into multiple word items (tokens). This is the first step in text preprocessing, splitting raw text into words or symbols that can be further processed.
		tokeniser = Tokeniser.getTokeniser();
		
	}
	
	/**
	 * Returns an array of processed terms for an input text string
	 * Perform text pre-processing
	 * @param text
	 * @return
	 */
	public List<String> process(String text) {
		
		// The input text text will be processed by word splitting and the result will be stored in the string array inputTokens.
		String[] inputTokens = tokeniser.getTokens(text);
		
		// If inputTokens is null (meaning that no word items were generated), then an empty ArrayList is returned
		if (inputTokens==null) return new ArrayList<String>(0);
		
		// The outTokens stores the processed word items, and its initial capacity is set to the length of the inputTokens.
		List<String> outTokens = new ArrayList<String>(inputTokens.length);
		for (int i =0; i<inputTokens.length; i++) {
			// Deactivation filtering and stemming
			String processedTerm = termProcessingPipeline.pipelineTerm(inputTokens[i]);
			// If the processed lexical item is null (possibly because the lexical item was removed by the deactivated word filter), the lexical item is skipped.
			if (processedTerm==null) continue;
			outTokens.add(processedTerm);
		}
		
		return outTokens;
	}
	
}
