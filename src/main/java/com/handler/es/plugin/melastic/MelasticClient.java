package com.handler.es.plugin.melastic;

import com.handler.es.plugin.exception.MElasticRunTimeException;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Created by liuxun on 2017/3/2.
 */
public class MelasticClient extends MelasticAction implements MelasticService {
    public static final Logger log = LoggerFactory.getLogger(MelasticClient.class);
    private TransportClient transportClient;

    /**
     * construct rely on
     * @param transportClient
     */
    public MelasticClient(TransportClient transportClient){
        this.transportClient = transportClient;
    }

    @Override
    public int indexCreate(String indexName, String typeName, Object source) {
        mappingSetting(transportClient,indexName,typeName,source);//设置mapping
        String indexVal = convertSourceToMelasticIndex(source);
        String indexId = getIndexIdFromSource(source);
        int i=0;
        try {
            IndexResponse response=null;
            if(StringUtils.isNotBlank(indexId)){
                 response = transportClient.prepareIndex(indexName, typeName, indexId)
                        .setSource(indexVal)
                        .execute()
                        .actionGet();
            }else{
                response=transportClient.prepareIndex(indexName,typeName)//参数说明： 索引，类型 ，_id
                        .setSource(indexVal)//setSource可以传以上map string  byte[] 几种方式
                        .execute()//execute操作默认都是异步的，可通过设置operationThreaded为false采用同步执行。
                        .actionGet();
            }
            if (response.isCreated()){
                i=i+1;
            }
        } catch (Exception e) {
            log.error("indexCreate error:",e);
        }
        return i;
    }

    /**
     * @param indexName indexName
     * @param typeName indexType
     * @param collections collection data ready to index
     * @return
     */
    @Override
    public int indexListCreate(String indexName, String typeName,Collection<?> collections) {
        int successResult = 0;
        try {
            BulkRequestBuilder bulkRequest = transportClient.prepareBulk();
            for (Object source: collections) {
                successResult++;
                String indexVal = convertSourceToMelasticIndex(source);
                String indexId = getIndexIdFromSource(source);
                LOG.debug("[createIndex] [bulkRequest] indexName:{}, typeName:{}, indexId:{}, indexValue:{}", new Object[]{indexName,typeName,indexId,indexVal});
                mappingSetting(transportClient,indexName,typeName,source);//设置mapping
                if(StringUtils.isNotBlank(indexId)){
                    bulkRequest.add(transportClient.prepareIndex(indexName, typeName, indexId).setSource(indexVal));
                }else{
                    bulkRequest.add(transportClient.prepareIndex(indexName, typeName).setSource(indexVal));
                }
            }
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (!bulkResponse.hasFailures()) {
                return successResult;
            } else {
                LOG.error("[createIndex] [bulkRequest] indexName:{}, typeName:{}, error:{}", new Object[]{
                        indexName,typeName,bulkResponse.buildFailureMessage()});
            }
        } catch (Exception e) {
            throw new MElasticRunTimeException("indexListCreate error:"+e.getMessage());
        }
        return successResult;
    }
    @Override
    public int delete(String indexName, String typeName, String id){
        LOG.debug("[delete] indexName:{}, typeName:{}, indexId:{}", new Object[]{indexName,typeName,id});
        int successResult = 0;
        try {
            DeleteResponse response = transportClient.prepareDelete(indexName,typeName,id).execute().actionGet();
            if (response.isFound()){
                successResult = successResult + 1;
            }
            LOG.info("response.getId():" + response.getId());
            LOG.info("response.isFound():" + response.isFound()); // 返回索引是否存在,存在删除
        } catch (Exception e) {
            throw new MElasticRunTimeException("delete error:"+e.getMessage());
        }
        return successResult;
    }

    public int deleteByIds(String indexName, String typeName, Collection<String> ids) {
        LOG.debug("[delete] indexName:{}, typeName:{}, indexIds:{}", new Object[]{indexName,typeName, ids});
        int successResult = 0;
        if( ids == null || ids.isEmpty() ){
            return 0;
        }
        for(String id : ids){
            int result = delete(indexName, typeName, id);
            if (result!=0){
                successResult  = successResult + 1;
            }
        }
        return successResult;
    }

    /**
     * 修改索引
     * @param indexName
     * @param typeName
     * @param source
     */
    public int updateIndex(String indexName, String typeName, Object source){
        LOG.debug("[updateIndex] indexName:{}, typeName:{}, source:{}", new Object[]{indexName,typeName, convertSourceToMelasticIndex(source)});
        int successResult = 0;
        try {
            String indexVal = convertSourceToMelasticIndex(source);
            String indexId = getIndexIdFromSource(source);
            UpdateRequest request = new UpdateRequest(indexName, typeName,indexId)
                    .doc(indexVal);
            UpdateResponse response = transportClient.update(request).get();
            LOG.debug("[updateIndex] response isCreated:"+response.isCreated());
            successResult  = successResult + 1;
        } catch (Exception e) {
            log.error("updateIndex error:",e);
        }
        return successResult;
    }

    public int updateIndex(String indexName, String typeName, Collection<?> collections) {
        LOG.debug("[update] indexName:{}, typeName:{}, objects:{}", new Object[]{indexName,typeName, convertSourceToMelasticIndex(collections)});
        int successResult = 0;
        if( collections == null || collections.isEmpty() ){
            return 0;
        }
        for(Object source : collections){
            int result=updateIndex(indexName,typeName,source);
            if (result!=0){
                successResult  = successResult + 1;
            }
        }
        return successResult;
    }

    public String select(String indexName, String typeName, String id) {
        LOG.debug("[select] indexName:{}, typeName:{}, indexId:{}", new Object[]{indexName,typeName,id});
        GetResponse response = transportClient.prepareGet(indexName, typeName, id).execute().actionGet();
        LOG.debug("response.getSource()：" + response.getSource());
        LOG.debug("response.getId():" + response.getId());
        LOG.debug("response.getSourceAsString():" + response.getSourceAsString());
        return response.getSourceAsString();
    }


    @Override
    public SearchResponse search(String indexName, String typeName, SearchType searchType,
                                 QueryBuilder queryBuilder, QueryBuilder filterBuilder,SortBuilder sort,Integer nowPage, Integer pageSize, Collection<String> highFields) {
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(indexName).setTypes(typeName);//查询器
        if( null != searchType ){//查询类型
            searchRequestBuilder.setSearchType(searchType);
        }
        if( null != queryBuilder ){//设置查询关键词
            searchRequestBuilder.setQuery(queryBuilder);
        }else{
            searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        }
        if( null != filterBuilder ){//过滤字段
            searchRequestBuilder.setPostFilter(filterBuilder);
        }
        if( null != nowPage ){// 分页应用
            searchRequestBuilder.setFrom(nowPage);
        }
        if( null != pageSize){
            searchRequestBuilder.setSize(pageSize);
        }
        if (null !=sort){
            searchRequestBuilder.addSort(sort);
        }
        if( !(null == highFields || highFields.isEmpty()) ){// 设置高亮显示
            for (String field : highFields) {
                searchRequestBuilder.addHighlightedField(field);
            }
            searchRequestBuilder.setHighlighterPreTags(HIGHLIGHTER_PRE_TAGS);
            searchRequestBuilder.setHighlighterPostTags(HIGHLIGHTER_END_TAGS);
        }
        SearchResponse searchResponse = searchRequestBuilder.setExplain(true)// 设置是否按查询匹配度排序
                .execute().actionGet();
        return searchResponse;
    }
}
