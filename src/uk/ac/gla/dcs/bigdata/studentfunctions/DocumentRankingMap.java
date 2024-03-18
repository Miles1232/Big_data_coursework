package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.DocumentQueryScore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * A custom MapFunction for converting Row objects into DocumentRanking objects.
 * It processes nested Row structures containing document query term scores,
 * extracting and transforming them into a structured DocumentRanking object.
 * The class is designed to ensure fast processing and includes steps for sorting 
 * results and filtering out redundancy among the documents.
 */
public class DocumentRankingMap implements MapFunction<Row, DocumentRanking> {
	

    /**
     * This method is invoked for each Row to convert it into a DocumentRanking object.
     * It extracts query details and document scores, and processes these to create a
     * ranked list of documents, sorted by their scores and filtered to remove redundancy.
     *
     * @param value The Row object to be processed, which contains embedded query information
     *              and associated document scores.
     * @return A DocumentRanking object representing the sorted and filtered results.
     * @throws Exception if any error occurs during the processing of the Row.
     */
    @Override
    public DocumentRanking call(Row value) throws Exception {
    	
//    	Query query =  value.getAs("query", Query.class);
    	
    	// Since it is not possible to get the structure object of the row directly as a Query, 
    	// it is taken out separately and populated into the newly created Query object.
    	
        Row queryRow = value.getAs("query");
        String originalQuery = queryRow.getAs("originalQuery");
        List<String> queryTerms = queryRow.getList(queryRow.fieldIndex("queryTerms"));
        List<Short> queryTermCountsList = queryRow.getList(queryRow.fieldIndex("queryTermCounts"));
        
        // Converting List<Short> to short[]
        short[] queryTermCounts = new short[queryTermCountsList.size()];
        for (int i = 0; i < queryTermCounts.length; i++) {
            queryTermCounts[i] = queryTermCountsList.get(i);
        }

        // Construct a Query object using the extracted values
        Query query = new Query(originalQuery, queryTerms, queryTermCounts);
//    	
//        System.out.println("------------------------------------------------");
//        System.out.println("query:"+query.getOriginalQuery());
//        
        List<String> docids = value.getList(1);
        List<String> titles = value.getList(2);
        List<Double> queryScores  = value.getList(3);
        
        List<DocumentQueryScore> dataset = new ArrayList<>();
        
        for (int i = 0; i < docids.size(); i++) {
            String docid = docids.get(i);
            String title = titles.get(i);
            Double queryScore = queryScores.get(i);

            DocumentQueryScore documentQueryScore = new DocumentQueryScore(docid, title, queryScore, query);

            dataset.add(documentQueryScore);
        }
        
        

        // carry out a conversion
        List<RankedResult> rankedResults = processDocuments(query, docids, titles, queryScores);

        return new DocumentRanking(query, rankedResults);
    }
    
    /**
     * Processes a list of document identifiers, titles, and query scores.
     * It removes duplicates and retains the highest-scoring documents.
     * The method implements an algorithm to manage a priority queue for efficient processing.
     * 
     * @param query The Query object containing the original query text and terms.
     * @param docids A list of document identifiers to be processed.
     * @param titles A list of document titles corresponding to the document identifiers.
     * @param queryScores A list of scores associated with the documents.
     * @return A list of RankedResult objects, representing the processed documents in ranked order.
     */
    private List<RankedResult> processDocuments(Query query, List<String> docids, List<String> titles, List<Double> queryScores) {
//    	System.out.println("Start processDocuments");
    	List<RankedResult> rankedResults = new ArrayList<>();
        
    	// Design algorithms to ensure that redundancy is removed while retaining the document with the highest DPH value
    	
    	int firstRemovals = 10; 
    	int everyRemovals = 2;
    	
    	List<DocumentQueryScore> totalList = new ArrayList<>(); // Used to place the remaining items after removing the empty header and the largest item. 
    	List<DocumentQueryScore> tempList = new ArrayList<>(); // Used to extract items that are prepended to the minHeapList to reduce the number of traversals through the summary table
    	List<DocumentQueryScore> minHeapList = new ArrayList<>(); // Used to store the top 10 DPH items

    	
    	// Priority queue, in descending order based on queryScore
    	PriorityQueue<DocumentQueryScore> priorityQueue = new PriorityQueue<>(
    			firstRemovals + everyRemovals, 
                (d2,d1) -> d2.getQueryScore().compareTo(d1.getQueryScore())
            );
    	
//    	DocumentQueryScore priorityQueueMin = new DocumentQueryScore(docids.get(0), titles.get(0), queryScores.get(0), query);
    	
//    	System.out.println("Start ergodic");
//    	System.out.println("docids.size():"+docids.size());
//    	System.out.println("titles.size():"+titles.size());
//    	System.out.println("queryScores.size():"+queryScores.size());
    	
    	// First traversal, extract the largest item, store it in the minHeapList and tempList
    	for (int i = 0; i < docids.size() - 1; i++) {

//    		System.out.println("i"+i);
    	    if (titles.get(i) != null && !titles.get(i).isEmpty()) {  // Ignore empty headings
//    	    	System.out.println("docids:"+docids.get(i)+" titles:"+titles.get(i)+" queryScores:"+queryScores.get(i));
    	        DocumentQueryScore dqs = new DocumentQueryScore(docids.get(i), titles.get(i), queryScores.get(i), query);
    	        
    	        // Maintenance of priority queues
    	        if (priorityQueue.size() < firstRemovals + everyRemovals) {
    	            priorityQueue.add(dqs);
//    	            System.out.println("first 15 add"+dqs.getDocid()+dqs.getQueryScore());
    	        } else {
    	            // Check if the current element score is higher than the smallest score in the queue, if so, replace it
    	            if (dqs.getQueryScore() > priorityQueue.peek().getQueryScore()) {
    	                priorityQueue.poll(); // Remove the current smallest scoring element
    	                priorityQueue.add(dqs); // Adding new elements
//    	                System.out.println("add after :"+i+" "+dqs.getQueryScore());
    	            } else {
    	                // Not enough points to go into PriorityQueue, add to totalList
    	                totalList.add(dqs);
//    	                System.out.println("add totalList :"+dqs.getDocid()+dqs.getQueryScore());
    	            }
    	        }
    	    }
    	} 	
    	
    	
        // Assign elements to minHeapList and tempList.
        while (!priorityQueue.isEmpty() && tempList.size() < everyRemovals) {
        	tempList.add(priorityQueue.poll());
        }

        while (!priorityQueue.isEmpty() && minHeapList.size() < firstRemovals) {
        	minHeapList.add(priorityQueue.poll());
        }// At this point the priorityQueue is empty and can be used again
       
        minHeapList.sort((o1, o2) -> o2.getQueryScore().compareTo(o1.getQueryScore()));  // descending order
        tempList.sort((o1, o2) -> o2.getQueryScore().compareTo(o1.getQueryScore()));  // descending order

        // Iterate over minHeapList to calculate the text distance.
        // In this process a certain sequence of calculations is followed to ensure that the largest DPH term is retained and redundancy is removed.
        for (int i = 0; i < minHeapList.size() - 1; i++) {
            for (int j = i + 1; j < minHeapList.size(); j++) {
                double distance = TextDistanceCalculator.similarity(minHeapList.get(i).getTitle(), minHeapList.get(j).getTitle());
//                System.out.println(i+" "+j+" i:"+i+" "+minHeapList.get(i).getQueryScore()+"j:"+j+" "+minHeapList.get(j).getQueryScore()+"distance:"+distance);
                if (distance < 0.5) {
//                	System.out.println("<0.5; j:"+j+" "+minHeapList.get(j).getQueryScore());
                    // Delete the element at position j
                    minHeapList.remove(j);
                    j--;
                    
                    // Supplement the minHeapList with new elements
                    replenishMinHeapListFromTempList(minHeapList, tempList, totalList, everyRemovals);
                    
                    // As soon as the element is replenished, the calculation of the distance between the new element and the item of the element 
                    // before the current i value is carried out to ensure that no element is missed in the calculation between elements
                    boolean minHeapList9correct = false;
                    
                    outerLoop:
                    while ( !minHeapList9correct ) {
                    	for (int k = 0; k < i; k++) {
                    		double distance9 = TextDistanceCalculator.similarity(minHeapList.get(k).getTitle(), minHeapList.get(minHeapList.size()-1).getTitle());
                            if (distance9 < 0.5) {
                            	replenishMinHeapListFromTempList(minHeapList, tempList, totalList, everyRemovals);
                            	continue outerLoop;  // If the distance is not enough, the current element will be deleted immediately to get a new element to compare again.
                            }
                        }
                    	// If the for loop completes without triggering continue, it means that no item with a distance less than 0.5 has been found
                    	// and set the flag to true to exit the while loop.
                        minHeapList9correct = true;
                    }
                    
                }
            }
        }
        
        return convertToRankedResults(minHeapList);
    }    
    
    /**
     * Replenishes the minHeapList with documents from the tempList and totalList.
     * This is done to maintain a set of documents ready for comparison and ranking.
     * 
     * @param minHeapList The list to be replenished, representing the current top documents.
     * @param tempList A temporary list used for storing documents.
     * @param totalList A comprehensive list of all documents.
     * @param everyRemovals The number of documents to be considered for each replenishment.
     */
    private static void replenishMinHeapListFromTempList(List<DocumentQueryScore> minHeapList, List<DocumentQueryScore> tempList, List<DocumentQueryScore> totalList, int everyRemovals) {
        if (!tempList.isEmpty()) {
        	// tempList is non-empty, directly added to minHeapList
        	
            minHeapList.add(tempList.remove(0));
//            System.out.println("miniHeapList add:"+minHeapList.get(9).getQueryScore());
            
        } else {  // empty tempList, traverse totalList to retake
        	
            PriorityQueue<DocumentQueryScore> tempPriorityQueue = new PriorityQueue<>(
                everyRemovals, 
                (d2, d1) -> d2.getQueryScore().compareTo(d1.getQueryScore())
            );
            
            // Fill tempPriorityQueue
            for (DocumentQueryScore dqs : totalList) {
                tempPriorityQueue.add(dqs);
                // Keep tempPriorityQueue size from exceeding everyRemovals.
                if (tempPriorityQueue.size() > everyRemovals) {
                    tempPriorityQueue.poll(); // Remove the element with the smallest score
                }
            }
            
//            System.out.println("tempPriorityQueue:");
          for (DocumentQueryScore document : tempPriorityQueue) {
//              System.out.println(document.getQueryScore());
          }

            // Remove elements of tempList from totalList
            totalList.removeAll(tempList);

            // Transferring elements from tempPriorityQueue to tempList
            tempList.addAll(tempPriorityQueue);
            
            minHeapList.add(tempList.remove(0));

            tempPriorityQueue.clear();
        }

    }
    
    /**
     * Converts a list of DocumentQueryScore objects into RankedResult objects.
     * This includes setting the document identifier, title, and score for each result.
     * 
     * @param minHeapList A list of DocumentQueryScore objects representing the documents to be converted.
     * @return A list of RankedResult objects corresponding to the DocumentQueryScore objects.
     */
    public List<RankedResult> convertToRankedResults(List<DocumentQueryScore> minHeapList) {
        List<RankedResult> rankedResults = new ArrayList<>();
        
//        int count = 0; 
        for (DocumentQueryScore documentQueryScore : minHeapList) {
//        	System.out.println("minHeapList:"+count);
        	
            RankedResult rankedResult = new RankedResult();
            
            rankedResult.setDocid(documentQueryScore.getDocid());
//            System.out.println("documentQueryScore.getDocid():"+documentQueryScore.getDocid());
            rankedResult.setTitle(documentQueryScore.getTitle());
//            System.out.println("documentQueryScore.getTitle():"+documentQueryScore.getTitle());
            rankedResult.setScore(documentQueryScore.getQueryScore());
//            System.out.println("documentQueryScore.getQueryScore():"+documentQueryScore.getQueryScore());

            rankedResults.add(rankedResult);
            
//            count++;
        }

        return rankedResults;
    }
}
