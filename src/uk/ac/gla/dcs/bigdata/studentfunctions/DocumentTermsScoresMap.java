package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentTermsScores;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentTermsStatistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * converts a DocumentTermsStatistics to a DocumentTermsScores object
 * calculate the DPH score for current document and every term within termList
 */
public class DocumentTermsScoresMap implements MapFunction<DocumentTermsStatistics, DocumentTermsScores> {
    /**
     * the number of documents in the corpus
     */
    long totalDocsInCorpus;
    /**
     * average length across all documents
     */
    double averageDocumentLengthInCorpus;
    /**
     * The map of the sum of different term frequencies for the term across all documents
     */
    Map<String, LongAccumulator> totalTermFrequencyInCorpusMap;

    /**
     * Constructs a new DocumentTermsScoresMap object.
     * @param totalDocsInCorpus the number of documents in the corpus
     * @param averageDocumentLengthInCorpus average length across all documents
     * @param totalTermFrequencyInCorpusMap The map of the sum of different term frequencies for the term across all documents
     */
    public DocumentTermsScoresMap(long totalDocsInCorpus, double averageDocumentLengthInCorpus, Map<String, LongAccumulator> totalTermFrequencyInCorpusMap) {
        this.totalDocsInCorpus = totalDocsInCorpus;
        this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
        this.totalTermFrequencyInCorpusMap = totalTermFrequencyInCorpusMap;
    }

    /**
     * converts a DocumentTermsStatistics to a DocumentTermsScores object
     * calculate the DPH score for current document and every term within termList
     */
    @Override
    public DocumentTermsScores call(DocumentTermsStatistics value) throws Exception {
        //current document ID
        String id = value.getId();
        //current document title
        String title = value.getTitle();
        //terms list
        List<String> termList = value.getTermList();
        //Term Frequency (count) of the term in the document
        List<Short> termFrequencyInCurrentDocumentList = value.getTermFrequencyInCurrentDocumentList();

        //result DPH scores list
        List<Double> scoresList = new ArrayList<>();

        for(int index=0;index<termList.size();index++){
            //The sum of term frequencies for the term across all documents
            int totalTermFrequencyInCorpus = totalTermFrequencyInCorpusMap.get(termList.get(index)).value().intValue();
            //Term Frequency (count) of the term in the document
            short termFrequencyInCurrentDocument = termFrequencyInCurrentDocumentList.get(index);
            //The length of the document (in terms)
            int currentDocumentLength = value.getCurrentDocumentLength();

            //if current document do not contain current term, the DPH score will be set to 0.0
            double score = 0.0;
            if(termFrequencyInCurrentDocument != 0) {
                score = DPHScorer.getDPHScore(termFrequencyInCurrentDocument,
                        totalTermFrequencyInCorpus,
                        currentDocumentLength,
                        averageDocumentLengthInCorpus,
                        totalDocsInCorpus);
            }

            scoresList.add(score);
        }

        return new DocumentTermsScores(id,termList,scoresList,title);
    }
}
