import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class SearchAll {
    public void search(String textToSearch, String sender, Client client) {
        SearchRequestBuilder srb1 = client
                .prepareSearch().setQuery(QueryBuilders.matchQuery("sender", sender));
        SearchRequestBuilder srb2 = client
                .prepareSearch().setQuery(QueryBuilders.matchQuery("message", textToSearch));

        MultiSearchResponse sr = client.prepareMultiSearch()
                .add(srb1)
                .add(srb2)
                .get();
        System.out.println(sr.toString());
    }
}
