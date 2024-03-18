package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.io.Serializable;

/**
 * DocumentQueryScore represents the association between a document and a query, along with the score
 * that quantifies the relevance or importance of the document for the given query.
 */
public class DocumentQueryScore implements Serializable{

	private static final long serialVersionUID = -6194902783579098501L;


	// current document id
    private String docid;
    
    // current title
    private String title;
    
    // current query score
    private Double queryScore;
    
    // current query
    private Query query;

    /**
     * Constructs a new DocumentQueryScore object with the given parameters.
     * @param docid The unique identifier for the document.
     * @param title The title of the document.
     * @param queryScore The calculated relevance score of the document for the query.
     * @param query The query object for which this score is calculated.
     */
    public DocumentQueryScore(String docid, String title, Double queryScore, Query query) {
        this.docid = docid;
        this.title = title;
        this.queryScore = queryScore;
        this.query = query;
    }

    // Getter and setter methods for the document ID
    public String getDocid() {
        return docid;
    }

    public void setDocid(String docid) {
        this.docid = docid;
    }

    // Getter and setter methods for the document title
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    // Getter and setter methods for the query score
    public Double getQueryScore() {
        return queryScore;
    }

    public void setQueryScore(Double queryScore) {
        this.queryScore = queryScore;
    }

    // Getter and setter methods for the query object
    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }
}
