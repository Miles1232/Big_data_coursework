package uk.ac.gla.dcs.bigdata.providedstructures;

import java.io.Serializable;

/**
 * Storing sorted results
 *
 */
public class RankedResult implements Serializable, Comparable<RankedResult> {

	private static final long serialVersionUID = -2905684103776472843L;
	
	String docid;
	// NewsArticle article;
	String title;
	double score;
	
	public RankedResult() {}
	
	public RankedResult(String docid, String title, double score) {
		super();
		this.docid = docid;
		this.title = title;
		// this.article = article;
		this.score = score;
	}

	public String getDocid() {
		return docid;
	}

	public void setDocid(String docid) {
		this.docid = docid;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

//	public NewsArticle getArticle() {
//		return article;
//	}
//
//	public void setArticle(NewsArticle article) {
//		this.article = article;
//	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	@Override
	public int compareTo(RankedResult o) {
		//return new Double(score).compareTo(o.score); // The constructor Double(double) is deprecated since version 9
		return Double.compare(score, o.score);
		// Negative: first < second
		// Zero: first == second
		// Positive: first > second

	}
	
	
	
}
