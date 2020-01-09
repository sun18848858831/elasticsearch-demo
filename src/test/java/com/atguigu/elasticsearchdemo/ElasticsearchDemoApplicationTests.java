package com.atguigu.elasticsearchdemo;

import com.alibaba.fastjson.JSON;
import com.atguigu.elasticsearchdemo.pojo.User;
import com.atguigu.elasticsearchdemo.repository.UserRepository;
import com.fasterxml.jackson.databind.annotation.JsonAppend;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@SpringBootTest
class ElasticsearchDemoApplicationTests {

    @Autowired
    private ElasticsearchRestTemplate restTemplate;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    void contextLoads() {
        this.restTemplate.createIndex(User.class);
        this.restTemplate.putMapping(User.class);
    }

    @Test
    void testDocument(){

        //this.userRepository.save(new User(1l,"蔡徐坤,唱跳rap打篮球",20,"123456"));

        List<User> users = new ArrayList<>();
        users.add(new User(1l, "柳岩", 18, "123456"));
        users.add(new User(2l, "范冰冰", 19, "123456"));
        users.add(new User(3l, "李冰冰", 20, "123456"));
        users.add(new User(4l, "锋哥", 21, "123456"));
        users.add(new User(5l, "小鹿", 22, "654321"));
        users.add(new User(6l, "韩红", 23, "654321"));
        users.add(new User(6l, "韩冰冰", 23, "654321"));
        this.userRepository.saveAll(users);
    }

    @Test
    void testDelete(){
        this.userRepository.deleteById(6l);
    }

    @Test
    void testQuery(){
        //System.out.println(this.userRepository.findById(4l));
        //System.out.println(this.userRepository.findAllById(Arrays.asList(1l, 2l, 3l)));
        //this.userRepository.findAllById(Arrays.asList(1l,2l,3l)).forEach(System.out::println);
        //this.userRepository.findByAgeBetween(19,21).forEach(System.out::println);
        this.userRepository.findByNative(19,21).forEach(System.out::println);
    }

    @Test
    void testSearch(){
        //this.userRepository.search(QueryBuilders.rangeQuery("age").gte(19).lte(22)).forEach(System.out::println);
//        Page<User> page = this.userRepository.search(QueryBuilders.rangeQuery("age").gte(19).lte(22), PageRequest.of(1, 2));
//        System.out.println(page.getTotalElements());
//        System.out.println(page.getTotalPages());
//        page.getContent().forEach(System.out::println);

        //初始化自定义查询构建器
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withQuery(QueryBuilders.matchQuery("name","冰冰").operator(Operator.AND));
        queryBuilder.withSort(SortBuilders.fieldSort("age").order(SortOrder.DESC));
        queryBuilder.withPageable(PageRequest.of(0,2));
        queryBuilder.withHighlightBuilder(new HighlightBuilder().field("name").preTags("<em>").postTags("</em>"));
        queryBuilder.addAggregation(AggregationBuilders.terms("passwordAgg").field("password"));
        AggregatedPage<User> page = (AggregatedPage)this.userRepository.search(queryBuilder.build());
        System.out.println(page.getTotalElements());
        System.out.println(page.getTotalPages());
        page.getContent().forEach(System.out::println);
        ParsedStringTerms terms = (ParsedStringTerms)page.getAggregation("passwordAgg");
        terms.getBuckets().forEach(bucket -> {
            System.out.println(bucket.getKeyAsString());
        });

    }


    @Test
    void testSearch2(){
        //初始化自定义查询构建器
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withQuery(QueryBuilders.matchQuery("name","冰冰").operator(Operator.AND));
        queryBuilder.withSort(SortBuilders.fieldSort("age").order(SortOrder.DESC));
        queryBuilder.withPageable(PageRequest.of(0,2));
        queryBuilder.withHighlightBuilder(new HighlightBuilder().field("name").preTags("<em>").postTags("</em>"));
        queryBuilder.addAggregation(AggregationBuilders.terms("passwordAgg").field("password"));
        this.restTemplate.query(queryBuilder.build(),response->{
            SearchHit[] hits = response.getHits().getHits();
            for(SearchHit hit : hits){
                String userJson = hit.getSourceAsString();
                User user = JSON.parseObject(userJson, User.class);
                System.out.println(user);
                //System.out.println(userJson);
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField highlightField = highlightFields.get("name");
                user.setName(highlightField.getFragments()[0].string());
                System.out.println(user);
            }

            Map<String, Aggregation> asMap = response.getAggregations().getAsMap();
            ParsedStringTerms passwordAgg = (ParsedStringTerms)asMap.get("passwordAgg");
            passwordAgg.getBuckets().forEach(bucker->System.out.println(bucker.getKeyAsString()));

            return null;
        });
    }

    @Test
    public void testHighLevelClient() throws IOException {

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("name","冰冰").operator(Operator.AND));
        sourceBuilder.sort("age",SortOrder.DESC);
        sourceBuilder.from(0);
        sourceBuilder.size(2);
        sourceBuilder.highlighter(new HighlightBuilder().field("name").preTags("<em>").postTags("</em>"));
        sourceBuilder.aggregation(AggregationBuilders.terms("passwordAgg").field("password")
                .subAggregation(AggregationBuilders.avg("ageAgg").field("age")));
        SearchResponse response = this.restHighLevelClient.search(new SearchRequest(new String[]{"user"}, sourceBuilder), RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        for(SearchHit hit : hits){
            String userJson = hit.getSourceAsString();
            User user = JSON.parseObject(userJson, User.class);
            System.out.println(user);
            //System.out.println(userJson);
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("name");
            user.setName(highlightField.getFragments()[0].string());
            System.out.println(user);
        }

        Map<String, Aggregation> asMap = response.getAggregations().getAsMap();
        ParsedStringTerms passwordAgg = (ParsedStringTerms)asMap.get("passwordAgg");
        passwordAgg.getBuckets().forEach(bucker->System.out.println(bucker.getKeyAsString()));

    }

}
