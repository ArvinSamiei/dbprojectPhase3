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

public class Search {
    public void searchAll(String textToSearch, String sender, Client client) {
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("sender", sender))
                .filter(QueryBuilders.termsQuery("message", textToSearch));
        SearchResponse resp = client.prepareSearch("messages").setQuery(query).get();
        for (SearchHit searchHitFields : resp.getHits()) {
            System.out.println(searchHitFields.getSourceAsString());
        }

    }

    public void search_spec(String textToSearch, String sender, String receiver, Client client){
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("sender", sender))
                .filter(QueryBuilders.termsQuery("receiver", receiver))
                .filter(QueryBuilders.termsQuery("message", textToSearch));
        SearchResponse resp = client.prepareSearch("messages").setQuery(query).get();
        for (SearchHit searchHitFields : resp.getHits()) {
            System.out.println(searchHitFields.getSourceAsString());
        }
    }

    public void search_group_by_sender(String id, String sender, String textToSearch, Client client){
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("id", id))
                .filter(QueryBuilders.termsQuery("sender", sender))
                .filter(QueryBuilders.termsQuery("message", textToSearch));
        SearchResponse resp = client.prepareSearch("messages2").setQuery(query).get();
        for (SearchHit searchHitFields : resp.getHits()) {
            System.out.println(searchHitFields.getSourceAsString());
        }
    }
}