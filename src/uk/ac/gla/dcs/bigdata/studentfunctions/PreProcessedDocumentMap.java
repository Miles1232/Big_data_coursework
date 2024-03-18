package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.PreProcessedDocument;

import java.util.List;

/**
 * converts a NewsArticle object to a PreProcessedDocument object
 * When calculating DPH for a document, you should only consider terms within the title and within
 * ContentItem elements that have a non-null subtype and that subtype is listed as 'paragraph'.
 * Additionally, if an article has more than 5 paragraphs, you need only consider the first 5.
 */
public class PreProcessedDocumentMap implements MapFunction<NewsArticle, PreProcessedDocument> {

	private static final long serialVersionUID = -8773343205717513734L;

    /**
     * converts a NewsArticle object to a PreProcessedDocument object
     * @param value input NewsArticle object
     * @return output PreProcessedDocument object
     * @throws Exception
     */
	@Override
    public PreProcessedDocument call(NewsArticle value) throws Exception {

        //news article id
        String id = value.getId();
        //news article title
        String title = value.getTitle();

        //only consider the first 5 paragraphs
        int paragraphCount = 0;
        StringBuilder sb = new StringBuilder();
        
        //append the string within the title
        if(title != null) {
        	 sb.append(title);
        }

        for (ContentItem contentItem : value.getContents()){
            //ContentItem elements that have a non-null subtype
            if (contentItem != null && contentItem.getSubtype() != null
                    //ContentItem elements that subtype is listed as 'paragraph'
                    && contentItem.getSubtype().equals("paragraph")){
                sb.append(" ").append(contentItem.getContent());
                paragraphCount++;
            }
            //only consider the first 5 paragraphs
            if (paragraphCount >= 5) {
            	break;
            }
        }

        //converts a piece of text to its tokenised, stopword removed and stemmed form
        List<String> terms = new TextPreProcessor().process(sb.toString());

        return new PreProcessedDocument(id, title, terms);
    }
}
