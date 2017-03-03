package com.handler.es.plugin.melastic;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import java.util.Collection;

/**
 * Created by liuxun on 2017/3/2.
 */
public interface MelasticService {
    /***
     * 创建索引
     * @param indexName
     * @param typeName
     * @param source
     * @return
     */
    int indexCreate(String indexName, String typeName, Object source);

    /***
     * 批量创建索引
     * @param indexName
     * @param typeName
     * @param collections
     * @return
     */
    int indexListCreate(String indexName, String typeName,Collection<?> collections);

    /**
     * 删除索引
     * @param indexName
     * @param typeName
     * @param id
     * @return
     */
    int delete(String indexName, String typeName, String id);

    /***
     * 批量删除
     * @param indexName
     * @param typeName
     * @param ids
     * @return
     */
    int deleteByIds(String indexName, String typeName, Collection<String> ids);

    /**
     * 修改索引
     * @param indexName
     * @param typeName
     * @param source
     * @return
     */
    int updateIndex(String indexName, String typeName, Object source);

    /**
     * 批量修改
     * @param indexName
     * @param typeName
     * @param collections
     * @return
     */
    int updateIndex(String indexName, String typeName, Collection<?> collections);

    /**
     * 根据ID查询
     * @param indexName
     * @param typeName
     * @param id
     * @return
     */
    String select(String indexName, String typeName, String id);

    /**
     * 搜索
     * @param indexName
     * @param typeName
     * @param searchType
     * @param queryBuilder
     * @param filterBuilder
     * @param sort
     * @param nowPage
     * @param pageSize
     * @param highFields
     * @return
     */
    SearchResponse search(String indexName, String typeName, SearchType searchType,
                          QueryBuilder queryBuilder, QueryBuilder filterBuilder, SortBuilder sort, Integer nowPage, Integer pageSize, Collection<String> highFields);

}
