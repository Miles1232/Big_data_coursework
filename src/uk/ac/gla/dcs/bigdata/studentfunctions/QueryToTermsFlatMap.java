package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import java.util.Iterator;

/**
 * extracts all terms within a query, transforms it to a string list
 */
public class QueryToTermsFlatMap implements FlatMapFunction<Query,String> {

	private static final long serialVersionUID = -460901918422011464L;

    /**
     * extracts all terms within a news article, transforms it to a string list
     * @param query input Query object
     * @return output a string list of all terms
     * @throws Exception
     */
	@Override
    public Iterator<String> call(Query query) throws Exception {
        return query.getQueryTerms().iterator();
    }
}
