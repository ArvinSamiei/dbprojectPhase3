import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class SearchAll {
    public void search(String textToSearch, String sender, Client client) {
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("sender", sender))
                .filter(QueryBuilders.termsQuery("message", textToSearch));
        SearchResponse resp = client.prepareSearch().setQuery(query).get();
        for (SearchHit searchHitFields : resp.getHits()) {
            System.out.println(searchHitFields.getSourceAsString());
        }

    }
}
