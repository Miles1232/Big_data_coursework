package uk.ac.gla.dcs.bigdata.studentstructures;
import java.util.List;
import java.io.Serializable;

public class DocumentTermsScores implements Serializable{
 
	/**
	 * Represents the scoring of a document based on specific terms.
	 * This class holds a document's ID, the list of terms extracted from the document,
	 * the corresponding scores for these terms, and the document's title.
	 */
	private static final long serialVersionUID = -8385141676543382068L;
	// Unique identifier for the document.
    private String id;
    
    // List of terms extracted from the document.
    private List<String> terms;
    
    // List of scores corresponding to the terms. Each score represents the relevance or importance of the term within the document.
    private List<Double> scores;
    
    // Title of the document.
    private String title;
    /**
     * Constructs a new DocumentQueryTermsScores object.
     * 
     * @param id The unique identifier for the document.
     * @param terms The list of terms extracted from the document.
     * @param scores The list of scores corresponding to each term.
     * @param title The title of the document.
     */
	public DocumentTermsScores(String id, List<String> terms, List<Double> scores, String title) {
		super();
		this.id = id;
		this.terms = terms;
		this.scores = scores;
		this.title = title;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public List<String> getTerms() {
		return terms;
	}
	public void setTerms(List<String> terms) {
		this.terms = terms;
	}
	public List<Double> getScores() {
		return scores;
	}
	public void setScores(List<Double> scores) {
		this.scores = scores;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String titles) {
		this.title = titles;
	}
}
