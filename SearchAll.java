



public class SearchAll {
    public void search(String textToSearch, Client client){
        SearchResponse response = client.prepareSearch("messages")
                .setSearchType("pv")
                .setQuery(QueryBuilders.match)
                .get();
    }
}
