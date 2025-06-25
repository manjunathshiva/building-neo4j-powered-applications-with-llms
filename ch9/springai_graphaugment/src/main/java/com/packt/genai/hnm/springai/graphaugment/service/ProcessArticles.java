package com.packt.genai.hnm.springai.graphaugment.service;



import com.packt.genai.hnm.springai.graphaugment.config.RunConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessArticles implements Runnable, IRequest {
    private OpenAIEmbeddingModelService embeddingModelService ;
    private Neo4jService neo4jService ;
    private RunConfiguration configuration ;

    private String curStatus = "0 %" ;

    private boolean isComplete = false ;

    public ProcessArticles(
            OpenAIEmbeddingModelService embeddingModelService,
            Neo4jService neo4jService,
            RunConfiguration configuration
            ) {
        this.embeddingModelService = embeddingModelService;
        this.neo4jService = neo4jService;
        this.configuration = configuration ;
    }

    public String getCurStatus() {
        return curStatus ;
    }

    public boolean isComplete() {
        return isComplete;
    }

    @Override
    public void run() {
        try {
            System.out.println("Retrieving Data from Graph");
            List<EncodeRequest> dbData = neo4jService.getArticlesFromDB() ;
            System.out.println("Retrieved Data from Graph");
            int i = 0 ;
            int processingSize = dbData.size() ;
            List<Map<String, Object>> embeddings = new ArrayList<>() ;
            int batchSize = 100 ;
            List<String> inputData = new ArrayList<>() ;
            List<String> ids = new ArrayList<>() ;

            for( EncodeRequest request: dbData ) {
                if (i > 0 && i % batchSize == 0) {
                    System.out.println("Retrieving Batch embedding");
                    List<float[]> embedList = embeddingModelService.generateEmbeddingBatch(inputData) ;
                    System.out.println("Saving Embeddings to Graph : " + i);
                    for( int j = 0 ; j < embedList.size() ; j++ ) {
                        Map<String, Object> embedMap = new HashMap<>();
                        embedMap.put("id", ids.get(j));
                        embedMap.put("embedding", embedList.get(j));
                        embeddings.add(embedMap) ;
                    }
                    neo4jService.saveArticleEmbeddings(embeddings);
                    embeddings.clear();
                    ids.clear();
                    inputData.clear();
                    curStatus = ( ( i * 100.0 ) / processingSize ) + " %" ;
                }
                ids.add(request.getId()) ;
                inputData.add(request.getText()) ;
                i++ ;
            }

            if( inputData.size() > 0 ) {
                System.out.println("Retrieving Batch embedding");
                List<float[]> embedList = embeddingModelService.generateEmbeddingBatch(inputData) ;
                System.out.println("Saving Embeddings to Graph : " + i);
                for( int j = 0 ; j < embedList.size() ; j++ ) {
                    Map<String, Object> embedMap = new HashMap<>();
                    embedMap.put("id", ids.get(j));
                    embedMap.put("embedding", embedList.get(j));
                    embeddings.add(embedMap) ;
                }
                neo4jService.saveArticleEmbeddings(embeddings);
                embeddings.clear();
                ids.clear();
                inputData.clear();
                curStatus = ( ( i * 100.0 ) / processingSize ) + " %" ;
            }
            curStatus = "100 %" ;
        }catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Done");
        isComplete = true ;
    }
}
