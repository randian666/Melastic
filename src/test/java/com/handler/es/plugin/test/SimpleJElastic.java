package com.handler.es.plugin.test;

import com.google.gson.Gson;
import com.handler.es.plugin.melastic.MelasticClient;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by liuxun on 2017/3/2.
 */
public class SimpleJElastic {
    ApplicationContext context = new ClassPathXmlApplicationContext("spring-bean-elastic.xml");
    TransportClient transportClient = null;
    MelasticClient client=null;
    @Before
    public void init(){
        transportClient = (TransportClient) context.getBean("elasticClient");
        if(null != transportClient ){
            printf("init Elastic client finish!");
        }
        client=new MelasticClient(transportClient);
    }
    @Test
    public void createIndex(){
        User vmodel1 = new User(6,"兰陵王",new Date(),"一个人，没有同类斩草除根刀锋所划之地，便是疆土");
        int result = client.indexCreate("index_lx", "user", vmodel1);
        printf("创建索引结果result:"+result);
    }

    @Test
    public void indexListCreate(){
        User vmodel1 = new User(1,"刘勋4",new Date(),"典型的二分查找");
        User vmodel2 = new User(2,"刘勋5",new Date(),"选择排序");
        List<User> lists=new ArrayList<User>();
        lists.add(vmodel1);
        lists.add(vmodel2);
        int result = client.indexListCreate("index_lx", "user", lists);
        printf("批量创建索引结果result:"+result);
    }
    @Test
    public void delete(){
        List<String> ids=new ArrayList<String>();
        ids.add("3");
        ids.add("5");
//        int result = client.delete("index_lx", "user", "3");
        int result = client.deleteByIds("index_lx", "user", ids);
        printf("删除索引结果result:"+result);
    }
    @Test
    public void update(){
//        User vmodel1 = new User(1,"刘勋4",new Date(),"典型的二分查找");
//        User vmodel2 = new User(5,"刘勋5",new Date(),"选择排序");
//        List<User> lists=new ArrayList<User>();
//        lists.add(vmodel1);
//        lists.add(vmodel2);

        User vmodel1 = new User(5,"兰陵王",new Date(),"一个人，没有同类斩草除根刀锋所划之地，便是疆土");
        int result = client.updateIndex("index_lx", "user", vmodel1);
        printf("修改索引结果result:"+result);
    }
    @Test
    public void select(){
        String json = client.select("index_lx", "user", "5");
        System.out.println("查询结果 result:"+json);
    }

    @Test
    public void search(){
        Gson g= null;
        List<User> lists= null;
        try {
            g = new Gson();
            String key="兰陵王";//搜索关键字
            // 关键词转义
            QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder(QueryParser.escape(key));// 关键词转义
//        queryBuilder.analyzer("ik_smart");
            queryBuilder.field("name").field("message");//搜索字段

            List<String> list=new ArrayList<String>();//高亮字段
            list.add("name");

            ExistsQueryBuilder filtetQuery = QueryBuilders.existsQuery("name");
            FieldSortBuilder sortBuilder = SortBuilders.fieldSort("id").order(SortOrder.DESC);
            /***
             *
             QUERY_THEN_FETCH:查询是针对所有的块执行的，但返回的是足够的信息，而不是文档内容（Document）。结果会被排序和分级，基于此，只有相关的块的文档对象会被返回。由于被取到的仅仅是这些，故而返回的hit的大小正好等于指定的size。这对于有许多块的index来说是很便利的（返回结果不会有重复的，因为块被分组了）
             QUERY_AND_FETCH:最原始（也可能是最快的）实现就是简单的在所有相关的shard上执行检索并返回结果。每个shard返回一定尺寸的结果。由于每个shard已经返回了一定尺寸的hit，这种类型实际上是返回多个shard的一定尺寸的结果给调用者。
             DFS_QUERY_THEN_FETCH：与QUERY_THEN_FETCH相同，预期一个初始的散射相伴用来为更准确的score计算分配了的term频率。
             DFS_QUERY_AND_FETCH:与QUERY_AND_FETCH相同，预期一个初始的散射相伴用来为更准确的score计算分配了的term频率。
             */
            SearchResponse response = client.search("index_lx", "user", SearchType.DFS_QUERY_THEN_FETCH, queryBuilder, filtetQuery, sortBuilder, 0, 10, list);
            lists = new ArrayList<User>();
            for (SearchHit hit : response.getHits()) {
                //将文档中的每一个对象转换json串值
                String json = hit.getSourceAsString();
                //将json串值转换成对应的实体对象
                User vmodel = new ObjectMapper().readValue(json, User.class);

                vmodel.setName(getHighlightField(hit));
                lists.add(vmodel);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        printf(g.toJson(lists));
    }

    public void printf(String text){
        System.out.println("[TEST]" + text);
    }
    private String getHighlightField(SearchHit hit){
        //获取对应的高亮域
        Map<String, HighlightField> result = hit.highlightFields();
        if (result.size()>0){
            //从设定的高亮域中取得指定域
            HighlightField titleField = result.get("name");
            //取得定义的高亮标签
            Text[] titleTexts =  titleField.fragments();
            //为title串值增加自定义的高亮标签
            String detail = "";
            for(Text text : titleTexts){
                detail += text;
            }
            return detail;
        }
        return "";
    }
}
