package com.sdust.mall.publisher.service.impl;

import com.sdust.mall.publisher.service.ESService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author LinYue
 * @email ustb2021@126.com
 * @create 2021-03-26 21:29
 */
//将当前对象的创建交给Spring容器来管理
@Service
public class ESServiceImpl implements ESService {

    //将ES的客户端操作对象注入到Service中
    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        /*
            GET /mall2021_dau_info_2021-03-25-query/_search
            {
              "query": {
                "match_all": {}
              }
            }
        */
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new MatchAllQueryBuilder());
        String query = sourceBuilder.toString();

        String indexName = "mall2021_dau_info_" + date + "-query";
        Search search = new Search.Builder(query)
                .addIndex(indexName)
                .build();
        Long total = 0L;
        try {
            SearchResult searchResult = jestClient.execute(search);
            if (searchResult.getTotal() != null) {
                total = searchResult.getTotal();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败！");
        }
        return total;
    }

    @Override
    public Map<String, Long> getDauHour(String date) {
        /*
            GET /mall2021_dau_info_2021-03-25-query/_search
            {
              "aggs": {
                "groupBy_hr": {
                  "terms": {
                    "field": "hr",
                    "size": 24
                  }
                }
              }
            }
        */
        /*Map<String, Long> hourMap = new HashMap<>();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder  termsAggregationBuilder = new TermsAggregationBuilder("groupBy_hr", ValueType.LONG).field("hr").size(24);
        sourceBuilder.aggregation(termsAggregationBuilder);
        String query = sourceBuilder.toString();
        String indexName = "mall2021_dau_info_" + date + "-query";
        Search search = new Search.Builder(query)
                .addIndex(indexName)
                .build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            TermsAggregation termsAggregation = searchResult.getAggregations().getTermsAggregation("groupBy_hr");
            if (termsAggregation != null) {
                List<TermsAggregation.Entry> buckets = termsAggregation.getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    hourMap.put(bucket.getKey(),bucket.getCount());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败！");
        }

        return hourMap;*/
        String indexName = "mall2021_dau_info_" + date + "-query";
        //构造查询语句
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggBuilder =
                AggregationBuilders.terms("groupby_hr").field("hr").size(24);
        searchSourceBuilder.aggregation(aggBuilder);
        Search search = new
                Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType("_doc").build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //封装返回结果
            Map<String, Long> aggMap = new HashMap<>();
            if (searchResult.getAggregations().getTermsAggregation("groupby_hr") != null) {
                List<TermsAggregation.Entry> buckets =
                        searchResult.getAggregations().getTermsAggregation("groupby_hr").getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    aggMap.put(bucket.getKey(), bucket.getCount());
                }
            }
            return aggMap;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }

    }
}
