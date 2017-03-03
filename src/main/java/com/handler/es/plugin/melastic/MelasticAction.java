package com.handler.es.plugin.melastic;

import com.handler.es.plugin.annotation.JEAnalyzer;
import com.handler.es.plugin.annotation.MElasticColumn;
import com.handler.es.plugin.annotation.MElasticId;
import com.handler.es.plugin.exception.MElasticRunTimeException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Created by liuxun on 2017/3/2.
 */
public class MelasticAction {
    public static final Logger LOG = LoggerFactory.getLogger(MelasticAction.class);
    public static String HIGHLIGHTER_END_TAGS="</em>";
    public static String HIGHLIGHTER_PRE_TAGS="<em>";
    /**
     * 通过注解获取对象中的id
     * @param source
     * @return
     */
    public String getIndexIdFromSource(Object source) {
        if (source == null) return null;
        Field[] fields = source.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(MElasticId.class)) {
                try {
                    field.setAccessible(true);
                    Object name = field.get(source);
                    return name == null ? null : name.toString();
                } catch (IllegalAccessException e) {
                    LOG.error("Unhandled exception occurred while getting annotated id from source");
                }
            }
        }
        return null;
    }
    /**
     * 通过注解从对象中获取column并构建json
     * @param source
     * @return
     */
    public String convertSourceToMelasticIndex(Object source){
        if (null == source){
            return null;
        }
        try {
            XContentBuilder json = XContentFactory.jsonBuilder();
            Class clazz = source.getClass();
            Field[] fields = clazz.getDeclaredFields();
            json.startObject();
            for (Field field : fields) {
                if(field.isAnnotationPresent(MElasticId.class) || field.isAnnotationPresent(MElasticColumn.class)){
                    field.setAccessible(true);
                    String ppName = field.getName();
                    Object ppVal =  field.get(source);
                    json.field(ppName,ppVal);
                }
            }
            return json.endObject().string();
        } catch (Exception e) {
            LOG.error("scan annotation for object to elastic index error." + e, e);
        }
        return null;
    }
    /**
     * elastic mapping schema
     * @param transportClient
     * @param indexName
     * @param typeName
     * @param source
     */
    protected void mappingSetting(TransportClient transportClient, String indexName, String typeName, Object source){
        XContentBuilder mapping = objectForMapping(typeName,source);
        try {
            LOG.debug("[mapping]" + mapping.string());
        } catch (IOException e) {
            LOG.error("[mapping]" + e.getMessage());
        }
        if(!isIndexExists(transportClient,indexName)){
            transportClient.admin().indices().prepareCreate(indexName).addMapping(typeName,mapping).execute().actionGet();
        }else{
            PutMappingRequest mappingRequest = Requests.putMappingRequest(indexName).type(typeName).source(mapping);
            transportClient.admin().indices().putMapping(mappingRequest).actionGet();
        }
    }

    public XContentBuilder objectForMapping(String typeName,Object source){
        if (null == source){
            return null;
        }
        XContentBuilder mapping = null;
        try {
            mapping = XContentFactory.jsonBuilder();
            mapping.startObject().startObject(typeName).startObject("properties");
            Field[] fields = source.getClass().getDeclaredFields();
            for (Field field : fields) {
                String ppName = field.getName();
                String ppType = field.getType().toString();
                if(field.isAnnotationPresent(MElasticId.class)){
                    mapping.startObject(ppName).field("type",javaTypeForElasticType(ppType)).field("store", "yes").endObject();
                }
                if(field.isAnnotationPresent(MElasticColumn.class)){
                    MElasticColumn jElasticColumn =  field.getAnnotation(MElasticColumn.class);
                    mapping.startObject(ppName).field("type", javaTypeForElasticType(ppType));
                    if(jElasticColumn.instore()){
                        mapping.field("store", "yes");
                    }
                    if(jElasticColumn.analyzer() == JEAnalyzer.analyzed){
                        mapping.field("index", JEAnalyzer.analyzed);
                    }else if(jElasticColumn.analyzer() == JEAnalyzer.not_analyzed){
                        mapping.field("index", JEAnalyzer.not_analyzed);
                    }else if(jElasticColumn.analyzer() == JEAnalyzer.no){
                        mapping.field("index", JEAnalyzer.no);
                    }
                    mapping.endObject();
                }
            }
            mapping.endObject().endObject().endObject();
        } catch (IOException e) {
            LOG.error("scan annotation for construct elastic data mapping error");
        }
        return mapping;
    }

    protected String javaTypeForElasticType(String javaType){
        if( javaType.endsWith("Long") || javaType.endsWith("long") ){
            return "long";
        }
        if( javaType.endsWith("Integer") || javaType.endsWith("int")
                || javaType.endsWith("Short") || javaType.endsWith("short")
                || javaType.endsWith("Byte") || javaType.endsWith("byte") ){
            return "integer";
        }
        if( javaType.endsWith("Double") || javaType.endsWith("double") || javaType.endsWith("Float") || javaType.endsWith("float")
                || javaType.endsWith("BigDecimal") || javaType.endsWith("BigInteger") ) {
            return "double";
        }
        if( javaType.endsWith("Date") || javaType.endsWith("Timestamp") || javaType.endsWith("Time") ){
            return "date";
        }
        if( javaType.endsWith("Boolean") || javaType.endsWith("boolean") ){
            return "boolean";
        }
        return "string";
    }

    protected boolean isIndexExists(TransportClient transportClient, String... indexNames){
        boolean isExists;
        ActionFuture<IndicesExistsResponse> response =  transportClient.admin().indices().exists(new IndicesExistsRequest(indexNames));
        try {
            StringBuffer buffer = new StringBuffer("[");
            if(indexNames.length>0){
                for(int i=0; i<indexNames.length; i++){
                    if(i<= indexNames.length-1){
                        buffer.append(indexNames[i]).append(",");
                    }else{
                        buffer.append(indexNames[i]);
                    }
                }
            }
            buffer.append("]");

            isExists = response.get().isExists();
            LOG.debug("[indexExists] " + buffer.toString() + " is Exists:" + isExists);

        } catch (Exception e) {
            throw new MElasticRunTimeException(e.getMessage());
        }
        return isExists;
    }
}
