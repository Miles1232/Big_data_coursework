package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentTermsStatistics;
import uk.ac.gla.dcs.bigdata.studentstructures.PreProcessedDocument;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * converts a PreProcessedDocument object to a DocumentTermsStatistics objects
 * combine documents and all query terms
 * count the number of times the term appears in the document
 * count the length of the current document
 */
public class DocumentTermsStatisticsMap implements MapFunction<PreProcessedDocument, DocumentTermsStatistics> {
    private static final long serialVersionUID = 5215799828155555852L;
    /**
     * The total length of all documents
     */
    LongAccumulator totalDocumentLengthAccumulator;
    /**
     * all terms in all queries
     */
    Broadcast<Set<String>> broadcastQueryTerms;
    /**
     * The total number of documents in the corpus
     */
    LongAccumulator totalDocsInCorpusAccumulator;
    /**
     * The map of the sum of different term frequencies for the term across all documents
     */
    Map<String, LongAccumulator> totalTermFrequencyInCorpusMap;

    /**
     * Constructs a new DocumentTermsStatistics object.
     * @param totalDocumentLengthAccumulator The total length of all documents
     * @param broadcastQueryTerms all terms in all queries
     * @param totalDocsInCorpusAccumulator The total number of documents in the corpus
     * @param totalTermFrequencyInCorpusMap The map of the sum of different term frequencies for the term across all documents
     */
    public DocumentTermsStatisticsMap(LongAccumulator totalDocumentLengthAccumulator, Broadcast<Set<String>> broadcastQueryTerms, LongAccumulator totalDocsInCorpusAccumulator, Map<String, LongAccumulator> totalTermFrequencyInCorpusMap) {
        this.totalDocumentLengthAccumulator = totalDocumentLengthAccumulator;
        this.broadcastQueryTerms = broadcastQueryTerms;
        this.totalDocsInCorpusAccumulator = totalDocsInCorpusAccumulator;
        this.totalTermFrequencyInCorpusMap = totalTermFrequencyInCorpusMap;
    }

    /**
     * converts a PreProcessedDocument object to a DocumentTermsStatistics object
     * @param value input PreProcessedDocument object
     * @return output DocumentTermsStatistics object
     * @throws Exception
     */
    @Override
    public DocumentTermsStatistics call(PreProcessedDocument value) throws Exception {
        //all terms in all queries
        Set<String> queryTerms = broadcastQueryTerms.value();
        //all terms in current document
        List<String> documentTermsList = value.getTerms();

        //The total number of documents in the corpus
        totalDocsInCorpusAccumulator.add(1);
        //The total length of all documents
        totalDocumentLengthAccumulator.add(documentTermsList.size());

        //initialize new result DocumentTermsStatistics object
        DocumentTermsStatistics documentTermsStatistics = new DocumentTermsStatistics(
                value.getId(),value.getTitle(),documentTermsList.size());

        //a list of all query terms
        List<String> termList = documentTermsStatistics.getTermList();
        //a list of term frequency (count) of the term in the document
        List<Short> termFrequencyInCurrentDocumentList = documentTermsStatistics.getTermFrequencyInCurrentDocumentList();

        for(String queryTerm : queryTerms){
            //current term
            termList.add(queryTerm);

            //the term frequency (count) of the term in the document
            short termFrequencyInCurrentDocument = (short) Collections.frequency(documentTermsList, queryTerm);
            termFrequencyInCurrentDocumentList.add(termFrequencyInCurrentDocument);

            //add the count of the current term in the current document, and put it into the map
            totalTermFrequencyInCorpusMap.get(queryTerm).add(termFrequencyInCurrentDocument);
        }

        return documentTermsStatistics;
    }
}
