package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;

/**
 * a news article after preprocessing
 * article content text of its tokenised, stopword removed and stemmed form
 */
public class PreProcessedDocument implements Serializable {
	private static final long serialVersionUID = -3297446917502447662L;
    /**
     * document id
     */
	private String id;
    /**
     * document title
     */
    private String title;
    /**
     * article content text of its tokenised, stopword removed and stemmed form
     */
    private List<String> terms;

    /**
     * empty constructor
     */
    public PreProcessedDocument() {
    }

    /**
     * constructs a PreProcessedDocument object
     * @param id document id
     * @param title document title
     * @param terms article content text of its tokenised, stopword removed and stemmed form
     */
    public PreProcessedDocument(String id, String title, List<String> terms) {
        this.id = id;
        this.title = title;
        this.terms = terms;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setTerms(List<String> terms) {
        this.terms = terms;
    }

    public String getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public List<String> getTerms() {
        return terms;
    }
}
