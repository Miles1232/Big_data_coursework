package uk.ac.gla.dcs.bigdata.studentstructures;

import java.util.ArrayList;
import java.util.List;

/**
 * combine documents and all query terms
 * count part of the statistics
 */
public class DocumentTermsStatistics {
    /**
     * document id
     */
    private String id;
    /**
     * document title
     */
    private String title;
    /**
     * a list of all query terms
     */
    private List<String> termList;
    /**
     * The length of the document (in terms)
     */
    private int currentDocumentLength;
    /**
     * Term Frequency (count) of the term in the document
     */
    private List<Short> termFrequencyInCurrentDocumentList;

    /**
     * empty constructor
     */
    public DocumentTermsStatistics() {
    }

    /**
     * constructs a DocumentTermsStatistics object
     * @param id document id
     * @param title document title
     * @param currentDocumentLength The length of the document (in terms)
     */
    public DocumentTermsStatistics(String id, String title, int currentDocumentLength) {
        this.id = id;
        this.title = title;
        this.termList = new ArrayList<>();
        this.currentDocumentLength = currentDocumentLength;
        this.termFrequencyInCurrentDocumentList = new ArrayList<>();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<String> getTermList() {
        return termList;
    }

    public void setTermList(List<String> termList) {
        this.termList = termList;
    }

    public int getCurrentDocumentLength() {
        return currentDocumentLength;
    }

    public void setCurrentDocumentLength(int currentDocumentLength) {
        this.currentDocumentLength = currentDocumentLength;
    }

    public List<Short> getTermFrequencyInCurrentDocumentList() {
        return termFrequencyInCurrentDocumentList;
    }

    public void setTermFrequencyInCurrentDocumentList(List<Short> termFrequencyInCurrentDocumentList) {
        this.termFrequencyInCurrentDocumentList = termFrequencyInCurrentDocumentList;
    }
}
