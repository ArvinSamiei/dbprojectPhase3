import com.mysql.cj.protocol.Resultset;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class Search {
    DBConnection dbConnection = new DBConnection();

    public void searchAll(String textToSearch, String sender, Client client) {
        dbConnection.makeConnection();
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.termsQuery("sender", sender))
                .must(QueryBuilders.matchPhraseQuery("message", textToSearch));
        SearchResponse resp = client.prepareSearch("messages").setQuery(query).get();
        ArrayList<String> ress = new ArrayList<String>();
        for (SearchHit searchHitFields : resp.getHits()) {
            ress.add(searchHitFields.getSourceAsString());
        }

        try {
            for (String s : ress) {
                int senderInd = s.indexOf("sender");
                int receiverInd = s.indexOf("receiver");
                int messageInd = s.indexOf("message");
                int dateInd = s.indexOf("date");
                String senderID = s.substring(senderInd + 9, receiverInd - 3);
                String receiverID = s.substring(receiverInd + 11, messageInd - 3);
                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                String time = s.substring(dateInd + 7, dateInd + 17);
                String clock = s.substring(dateInd + 18, dateInd + 26);
                PreparedStatement senderName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                senderName.setString(1, senderID);
                ResultSet resultset = null;
                resultset = senderName.executeQuery();
                String nameOfSender = null;
                while (resultset != null && resultset.next()) {
                    nameOfSender = resultset.getString("name");
                }

                if (nameOfSender == null) {
                    nameOfSender = senderID;
                }

                PreparedStatement receiverName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                receiverName.setString(1, receiverID);
                resultset = null;
                resultset = receiverName.executeQuery();
                String nameOfReceiver = null;
                while (resultset != null && resultset.next()) {
                    nameOfReceiver = resultset.getString("name");
                }

                if (nameOfReceiver == null) {
                    nameOfReceiver = receiverID;
                }

                System.out.printf("Sender:%s Receiver:%s Time:\"%s %s\" Message:\"%s\"\n", nameOfSender, nameOfReceiver, time, clock, messageOfSender);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

        query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("receiver", sender))
                .must(QueryBuilders.matchPhraseQuery("message", textToSearch));
        resp = client.prepareSearch("messages").setQuery(query).get();
        ress.clear();
        for (SearchHit searchHitFields : resp.getHits()) {
            ress.add(searchHitFields.getSourceAsString());
        }

        try {
            for (String s : ress) {
                int senderInd = s.indexOf("sender");
                int receiverInd = s.indexOf("receiver");
                int messageInd = s.indexOf("message");
                int dateInd = s.indexOf("date");
                String senderID = s.substring(senderInd + 9, receiverInd - 3);
                String receiverID = s.substring(receiverInd + 11, messageInd - 3);
                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                String time = s.substring(dateInd + 7, dateInd + 17);
                String clock = s.substring(dateInd + 18, dateInd + 26);
                PreparedStatement senderName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                senderName.setString(1, senderID);
                ResultSet resultset = null;
                resultset = senderName.executeQuery();
                String nameOfSender = null;
                while (resultset != null && resultset.next()) {
                    nameOfSender = resultset.getString("name");
                }

                if (nameOfSender == null) {
                    nameOfSender = senderID;
                }

                PreparedStatement receiverName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                receiverName.setString(1, receiverID);
                resultset = null;
                resultset = receiverName.executeQuery();
                String nameOfReceiver = null;
                while (resultset != null && resultset.next()) {
                    nameOfReceiver = resultset.getString("name");
                }

                if (nameOfReceiver == null) {
                    nameOfReceiver = receiverID;
                }

                System.out.printf("Sender:%s Receiver:%s Time:\"%s %s\" Message:\"%s\"\n", nameOfSender, nameOfReceiver, time, clock, messageOfSender);
            }


        } catch (Exception e) {
            System.out.println(e);
        }

    }


    public void searchAllFuzzy(String textToSearch, String sender, Client client) {
        dbConnection.makeConnection();
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.termsQuery("sender", sender))
                .should(QueryBuilders.matchQuery("message", textToSearch).fuzziness(Fuzziness.AUTO).operator(Operator.AND));
        SearchResponse resp = client.prepareSearch("messages").setQuery(query).get();
        ArrayList<String> ress = new ArrayList<String>();
        for (SearchHit searchHitFields : resp.getHits()) {
            ress.add(searchHitFields.getSourceAsString());
        }

        try {
            for (String s : ress) {
                int senderInd = s.indexOf("sender");
                int receiverInd = s.indexOf("receiver");
                int messageInd = s.indexOf("message");
                int dateInd = s.indexOf("date");
                String senderID = s.substring(senderInd + 9, receiverInd - 3);
                String receiverID = s.substring(receiverInd + 11, messageInd - 3);
                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                String time = s.substring(dateInd + 7, dateInd + 17);
                String clock = s.substring(dateInd + 18, dateInd + 26);
                PreparedStatement senderName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                senderName.setString(1, senderID);
                ResultSet resultset = null;
                resultset = senderName.executeQuery();
                String nameOfSender = null;
                while (resultset != null && resultset.next()) {
                    nameOfSender = resultset.getString("name");
                }

                if (nameOfSender == null) {
                    nameOfSender = senderID;
                }

                PreparedStatement receiverName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                receiverName.setString(1, receiverID);
                resultset = null;
                resultset = receiverName.executeQuery();
                String nameOfReceiver = null;
                while (resultset != null && resultset.next()) {
                    nameOfReceiver = resultset.getString("name");
                }

                if (nameOfReceiver == null) {
                    nameOfReceiver = receiverID;
                }

                System.out.printf("Sender:%s Receiver:%s Time:\"%s %s\" Message:\"%s\"\n", nameOfSender, nameOfReceiver, time, clock, messageOfSender);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

        query = QueryBuilders.boolQuery()
                .must(QueryBuilders.termsQuery("receiver", sender))
                .should(QueryBuilders.matchQuery("message", textToSearch).fuzziness(Fuzziness.AUTO).operator(Operator.AND));
        resp = client.prepareSearch("messages").setQuery(query).get();
        ress.clear();
        for (SearchHit searchHitFields : resp.getHits()) {
            ress.add(searchHitFields.getSourceAsString());
        }

        try {
            for (String s : ress) {
                int senderInd = s.indexOf("sender");
                int receiverInd = s.indexOf("receiver");
                int messageInd = s.indexOf("message");
                int dateInd = s.indexOf("date");
                String senderID = s.substring(senderInd + 9, receiverInd - 3);
                String receiverID = s.substring(receiverInd + 11, messageInd - 3);
                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                String time = s.substring(dateInd + 7, dateInd + 17);
                String clock = s.substring(dateInd + 18, dateInd + 26);
                PreparedStatement senderName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                senderName.setString(1, senderID);
                ResultSet resultset = null;
                resultset = senderName.executeQuery();
                String nameOfSender = null;
                while (resultset != null && resultset.next()) {
                    nameOfSender = resultset.getString("name");
                }

                if (nameOfSender == null) {
                    nameOfSender = senderID;
                }

                PreparedStatement receiverName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                receiverName.setString(1, receiverID);
                resultset = null;
                resultset = receiverName.executeQuery();
                String nameOfReceiver = null;
                while (resultset != null && resultset.next()) {
                    nameOfReceiver = resultset.getString("name");
                }

                if (nameOfReceiver == null) {
                    nameOfReceiver = receiverID;
                }

                System.out.printf("Sender:%s Receiver:%s Time:\"%s %s\" Message:\"%s\"\n", nameOfSender, nameOfReceiver, time, clock, messageOfSender);
            }


        } catch (Exception e) {
            System.out.println(e);
        }

    }

    public void search_spec(String textToSearch, String sender, String receiver, Client client) {
        dbConnection.makeConnection();
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("sender", sender))
                .filter(QueryBuilders.termsQuery("receiver", receiver))
                .filter(QueryBuilders.matchPhraseQuery("message", textToSearch));
        SearchResponse resp = client.prepareSearch("messages").setQuery(query).get();
        ArrayList<String> ress = new ArrayList<String>();
        for (SearchHit searchHitFields : resp.getHits()) {
            ress.add(searchHitFields.getSourceAsString());
//            System.out.println();
        }

        try {
            for (String s : ress) {
                int senderInd = s.indexOf("sender");
                int receiverInd = s.indexOf("receiver");
                int messageInd = s.indexOf("message");
                int dateInd = s.indexOf("date");
                String senderID = s.substring(senderInd + 9, receiverInd - 3);
                String receiverID = s.substring(receiverInd + 11, messageInd - 3);
                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                String time = s.substring(dateInd + 7, dateInd + 17);
                String clock = s.substring(dateInd + 18, dateInd + 26);
                PreparedStatement senderName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                senderName.setString(1, senderID);
                ResultSet resultset = null;
                resultset = senderName.executeQuery();
                String nameOfSender = null;
                while (resultset != null && resultset.next()) {
                    nameOfSender = resultset.getString("name");
                }

                if (nameOfSender == null) {
                    nameOfSender = sender;
                }

                PreparedStatement receiverName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                receiverName.setString(1, receiverID);
                resultset = null;
                resultset = receiverName.executeQuery();
                String nameOfReceiver = null;
                while (resultset != null && resultset.next()) {
                    nameOfReceiver = resultset.getString("name");
                }

                if (nameOfReceiver == null) {
                    nameOfReceiver = receiverID;
                }

                System.out.printf("Sender:%s Receiver:%s Time:\"%s %s\" Message:\"%s\"\n", nameOfSender, nameOfReceiver, time, clock, messageOfSender);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("sender", receiver))
                .filter(QueryBuilders.termsQuery("receiver", sender))
                .filter(QueryBuilders.matchPhraseQuery("message", textToSearch));
        resp = client.prepareSearch("messages").setQuery(query).get();
        ress.clear();
        for (SearchHit searchHitFields : resp.getHits()) {
            ress.add(searchHitFields.getSourceAsString());
//            System.out.println();
        }

        try {
            for (String s : ress) {
                int senderInd = s.indexOf("sender");
                int receiverInd = s.indexOf("receiver");
                int messageInd = s.indexOf("message");
                int dateInd = s.indexOf("date");
                String senderID = s.substring(senderInd + 9, receiverInd - 3);
                String receiverID = s.substring(receiverInd + 11, messageInd - 3);
                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                String time = s.substring(dateInd + 7, dateInd + 17);
                String clock = s.substring(dateInd + 18, dateInd + 26);
                PreparedStatement senderName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                senderName.setString(1, senderID);
                ResultSet resultset = null;
                resultset = senderName.executeQuery();
                String nameOfSender = null;
                while (resultset != null && resultset.next()) {
                    nameOfSender = resultset.getString("name");
                }

                if (nameOfSender == null) {
                    nameOfSender = sender;
                }

                PreparedStatement receiverName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                receiverName.setString(1, receiverID);
                resultset = null;
                resultset = receiverName.executeQuery();
                String nameOfReceiver = null;
                while (resultset != null && resultset.next()) {
                    nameOfReceiver = resultset.getString("name");
                }

                if (nameOfReceiver == null) {
                    nameOfReceiver = receiverID;
                }

                System.out.printf("Sender:%s Receiver:%s Time:\"%s %s\" Message:\"%s\"\n", nameOfSender, nameOfReceiver, time, clock, messageOfSender);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void search_group_by_sender(String id, String sender, String textToSearch, Client client) {
        dbConnection.makeConnection();
        ArrayList<String> ress = new ArrayList<>();
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("id", id))
                .filter(QueryBuilders.termsQuery("sender", sender))
                .filter(QueryBuilders.matchPhraseQuery("message", textToSearch));
        SearchResponse resp = client.prepareSearch("messages2").setQuery(query).get();
        for (SearchHit searchHitFields : resp.getHits()) {
            ress.add(searchHitFields.getSourceAsString());
//            System.out.println();
        }

        try {
            for (String s : ress) {
                int senderInd = s.indexOf("sender");
                int messageInd = s.indexOf("message");
                int dateInd = s.indexOf("date");
                String senderID = s.substring(senderInd + 9, messageInd - 3);
                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                String time = s.substring(dateInd + 7, dateInd + 17);
                String clock = s.substring(dateInd + 18, dateInd + 26);
                PreparedStatement senderName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                senderName.setString(1, senderID);
                ResultSet resultset = null;
                resultset = senderName.executeQuery();
                String nameOfSender = null;
                while (resultset != null && resultset.next()) {
                    nameOfSender = resultset.getString("name");
                }

                if (nameOfSender == null) {
                    nameOfSender = sender;
                }
                System.out.printf("Sender:%s Time:\"%s %s\" Message:\"%s\"\n", nameOfSender, time, clock, messageOfSender);
            }


        } catch (Exception e) {
            System.out.println(e);
        }
    }


    public void view_messages_by_date(String user, String date, Client client) {
        dbConnection.makeConnection();
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.rangeQuery("date").gte(date + "T" + "00:00:00.000").lte(date + "T" + "23:59:59.999"))
                .filter(QueryBuilders.termsQuery("receiver", user));
        SearchResponse resp = client.prepareSearch("messages").setQuery(query).get();
        ArrayList<String> ress = new ArrayList<String>();
        ress.clear();
        for (SearchHit searchHitFields : resp.getHits()) {
            ress.add(searchHitFields.getSourceAsString());
        }

        try {
            for (String s : ress) {
                int senderInd = s.indexOf("sender");
                int receiverInd = s.indexOf("receiver");
                int messageInd = s.indexOf("message");
                int dateInd = s.indexOf("date");
                String senderID = s.substring(senderInd + 9, receiverInd - 3);
                String receiverID = s.substring(receiverInd + 11, messageInd - 3);
                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                String time = s.substring(dateInd + 7, dateInd + 17);
                String clock = s.substring(dateInd + 18, dateInd + 26);
                PreparedStatement senderName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                senderName.setString(1, senderID);
                ResultSet resultset = null;
                resultset = senderName.executeQuery();
                String nameOfSender = null;
                while (resultset != null && resultset.next()) {
                    nameOfSender = resultset.getString("name");
                }

                if (nameOfSender == null) {
                    nameOfSender = senderID;
                }

                PreparedStatement receiverName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                receiverName.setString(1, receiverID);
                resultset = null;
                resultset = receiverName.executeQuery();
                String nameOfReceiver = null;
                while (resultset != null && resultset.next()) {
                    nameOfReceiver = resultset.getString("name");
                }

                if (nameOfReceiver == null) {
                    nameOfReceiver = receiverID;
                }

                System.out.printf("Sender:%s Receiver:%s Time:\"%s %s\" Message:\"%s\"\n", nameOfSender, nameOfReceiver, time, clock, messageOfSender);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
        query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.rangeQuery("date").gte(date + "T" + "00:00:00.000").lte(date + "T" + "23:59:59.999"))
                .filter(QueryBuilders.termsQuery("sender", user));
        resp = client.prepareSearch("messages").setQuery(query).get();
        ress.clear();
        for (SearchHit searchHitFields : resp.getHits()) {
            ress.add(searchHitFields.getSourceAsString());
        }

        try {
            for (String s : ress) {
                int senderInd = s.indexOf("sender");
                int receiverInd = s.indexOf("receiver");
                int messageInd = s.indexOf("message");
                int dateInd = s.indexOf("date");
                String senderID = s.substring(senderInd + 9, receiverInd - 3);
                String receiverID = s.substring(receiverInd + 11, messageInd - 3);
                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                String time = s.substring(dateInd + 7, dateInd + 17);
                String clock = s.substring(dateInd + 18, dateInd + 26);
                PreparedStatement senderName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                senderName.setString(1, senderID);
                ResultSet resultset = null;
                resultset = senderName.executeQuery();
                String nameOfSender = null;
                while (resultset != null && resultset.next()) {
                    nameOfSender = resultset.getString("name");
                }

                if (nameOfSender == null) {
                    nameOfSender = senderID;
                }

                PreparedStatement receiverName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                receiverName.setString(1, receiverID);
                resultset = null;
                resultset = receiverName.executeQuery();
                String nameOfReceiver = null;
                while (resultset != null && resultset.next()) {
                    nameOfReceiver = resultset.getString("name");
                }

                if (nameOfReceiver == null) {
                    nameOfReceiver = receiverID;
                }

                System.out.printf("Sender:%s Receiver:%s Time:\"%s %s\" Message:\"%s\"\n", nameOfSender, nameOfReceiver, time, clock, messageOfSender);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            PreparedStatement groupsOfUser = dbConnection.connection.prepareStatement("select group_id from group_members where member_id=(?)");
            groupsOfUser.setString(1, user);
            ResultSet resultSet = null;
            resultSet = groupsOfUser.executeQuery();
            ArrayList<String> groupIDs = new ArrayList<String>();
            while (resultSet != null && resultSet.next()) {
                groupIDs.add(resultSet.getString("group_id"));
            }

            for (String groupID : groupIDs) {
                query = QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termsQuery("id", groupID))
                        .filter(QueryBuilders.rangeQuery("date").gte(date + "T" + "00:00:00.000").lte(date + "T" + "23:59:59.999"));
                resp = client.prepareSearch("messages2").setQuery(query).get();
                ress.clear();
                for (SearchHit searchHitFields : resp.getHits()) {
                    ress.add(searchHitFields.getSourceAsString());
                }

                try {
                    for (String s : ress) {
                        int senderInd = s.indexOf("sender");
                        int messageInd = s.indexOf("message");
                        int dateInd = s.indexOf("date");
                        String senderID = s.substring(senderInd + 9, messageInd - 3);
                        String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                        String time = s.substring(dateInd + 7, dateInd + 17);
                        String clock = s.substring(dateInd + 18, dateInd + 26);
                        PreparedStatement senderName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                        senderName.setString(1, senderID);
                        ResultSet resultset = null;
                        resultset = senderName.executeQuery();
                        String nameOfSender = null;
                        while (resultset != null && resultset.next()) {
                            nameOfSender = resultset.getString("name");
                        }

                        if (nameOfSender == null) {
                            nameOfSender = senderID;
                        }
                        System.out.printf("Sender:%s Time:\"%s %s\" Message:\"%s\"\n", nameOfSender, time, clock, messageOfSender);
                    }


                } catch (Exception e) {
                    System.out.println(e);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }


        try {
            PreparedStatement channelsOfUser = dbConnection.connection.prepareStatement("select channel_id from channel_members where member_id=(?)");
            channelsOfUser.setString(1, user);
            ResultSet resultSet = null;
            resultSet = channelsOfUser.executeQuery();
            ArrayList<String> channelIDs = new ArrayList<String>();
            while (resultSet != null && resultSet.next()) {
                channelIDs.add(resultSet.getString("channel_id"));
            }

            for (String channelID : channelIDs) {
                query = QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termsQuery("id", channelID))
                        .filter(QueryBuilders.rangeQuery("date").gte(date + "T" + "00:00:00.000").lte(date + "T" + "23:59:59.999"));
                resp = client.prepareSearch("messages3").setQuery(query).get();

                ress.clear();
                for (SearchHit searchHitFields : resp.getHits()) {
                    ress.add(searchHitFields.getSourceAsString());
                }

                try {
                    for (String s : ress) {
                        int messageInd = s.indexOf("message");
                        int dateInd = s.indexOf("date");
                        String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                        String time = s.substring(dateInd + 7, dateInd + 17);
                        String clock = s.substring(dateInd + 18, dateInd + 26);
                        System.out.printf("Time:\"%s %s\" Message:\"%s\"\n", time, clock, messageOfSender);
                    }


                } catch (Exception e) {
                    System.out.println(e);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }


    }


    public void view_messages_by_date_group(String groupID, String date, Client client) {
        dbConnection.makeConnection();
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("id", groupID))
                .filter(QueryBuilders.rangeQuery("date").gte(date + "T" + "00:00:00.000").lte(date + "T" + "23:59:59.999"));
        SearchResponse resp = client.prepareSearch("messages2").setQuery(query).get();
        ArrayList<String> ress = new ArrayList<String>();
        for (SearchHit searchHitFields : resp.getHits()) {
            ress.add(searchHitFields.getSourceAsString());
        }

        try {
            for (String s : ress) {
                int senderInd = s.indexOf("sender");
                int messageInd = s.indexOf("message");
                int dateInd = s.indexOf("date");
                String senderID = s.substring(senderInd + 9, messageInd - 3);
                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                String time = s.substring(dateInd + 7, dateInd + 17);
                String clock = s.substring(dateInd + 18, dateInd + 26);
                PreparedStatement senderName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                senderName.setString(1, senderID);
                ResultSet resultset = null;
                resultset = senderName.executeQuery();
                String nameOfSender = null;
                while (resultset != null && resultset.next()) {
                    nameOfSender = resultset.getString("name");
                }

                if (nameOfSender == null) {
                    nameOfSender = senderID;
                }
                System.out.printf("Sender:%s Time:\"%s %s\" Message:\"%s\"\n", nameOfSender, time, clock, messageOfSender);
            }


        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void search_group(String groupID, String textToSearch, Client client) {
        dbConnection.makeConnection();
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("id", groupID))
                .filter(QueryBuilders.matchPhraseQuery("message", textToSearch));
        SearchResponse resp = client.prepareSearch("messages2").setQuery(query).get();
        ArrayList<String> ress = new ArrayList<String>();
        for (SearchHit searchHitFields : resp.getHits()) {
            ress.add(searchHitFields.getSourceAsString());
//            System.out.println();
        }

        try {
            for (String s : ress) {
                int senderInd = s.indexOf("sender");
                int messageInd = s.indexOf("message");
                int dateInd = s.indexOf("date");
                String senderID = s.substring(senderInd + 9, messageInd - 3);
                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                String time = s.substring(dateInd + 7, dateInd + 17);
                String clock = s.substring(dateInd + 18, dateInd + 26);
                PreparedStatement senderName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                senderName.setString(1, senderID);
                ResultSet resultset = null;
                resultset = senderName.executeQuery();
                String nameOfSender = null;
                while (resultset != null && resultset.next()) {
                    nameOfSender = resultset.getString("name");
                }

                if (nameOfSender == null) {
                    nameOfSender = senderID;
                }
                System.out.printf("Sender:%s Time:\"%s %s\" Message:\"%s\"\n", nameOfSender, time, clock, messageOfSender);
            }


        } catch (Exception e) {
            System.out.println(e);
        }

    }

    public void search_channel(String id, String textToSearch, Client client) {
        dbConnection.makeConnection();
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("id", id))
                .filter(QueryBuilders.matchPhraseQuery("message", textToSearch));
        SearchResponse resp = client.prepareSearch("messages3").setQuery(query).get();

        ArrayList<String> ress = new ArrayList<String>();
        for (SearchHit searchHitFields : resp.getHits()) {
            ress.add(searchHitFields.getSourceAsString());
        }

        try {
            for (String s : ress) {
                int messageInd = s.indexOf("message");
                int dateInd = s.indexOf("date");
                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                String time = s.substring(dateInd + 7, dateInd + 17);
                String clock = s.substring(dateInd + 18, dateInd + 26);
                System.out.printf("Time:\"%s %s\" Message:\"%s\"\n", time, clock, messageOfSender);
            }


        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void view_messages_by_date_chat(String user, String id, String date, Client client) {
        dbConnection.makeConnection();
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.rangeQuery("date").gte(date + "T" + "00:00:00.000").lte(date + "T" + "23:59:59.999"))
                .filter(QueryBuilders.termsQuery("receiver", user))
                .filter(QueryBuilders.termsQuery("sender", id));
        SearchResponse resp = client.prepareSearch("messages").setQuery(query).get();
        ArrayList<String> ress = new ArrayList<String>();
        ress.clear();
        for (SearchHit searchHitFields : resp.getHits()) {
            ress.add(searchHitFields.getSourceAsString());
        }

        try {
            for (String s : ress) {
                int senderInd = s.indexOf("sender");
                int receiverInd = s.indexOf("receiver");
                int messageInd = s.indexOf("message");
                int dateInd = s.indexOf("date");
                String senderID = s.substring(senderInd + 9, receiverInd - 3);
                String receiverID = s.substring(receiverInd + 11, messageInd - 3);
                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                String time = s.substring(dateInd + 7, dateInd + 17);
                String clock = s.substring(dateInd + 18, dateInd + 26);
                PreparedStatement senderName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                senderName.setString(1, senderID);
                ResultSet resultset = null;
                resultset = senderName.executeQuery();
                String nameOfSender = null;
                while (resultset != null && resultset.next()) {
                    nameOfSender = resultset.getString("name");
                }

                if (nameOfSender == null) {
                    nameOfSender = senderID;
                }

                PreparedStatement receiverName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                receiverName.setString(1, receiverID);
                resultset = null;
                resultset = receiverName.executeQuery();
                String nameOfReceiver = null;
                while (resultset != null && resultset.next()) {
                    nameOfReceiver = resultset.getString("name");
                }

                if (nameOfReceiver == null) {
                    nameOfReceiver = receiverID;
                }

                System.out.printf("Sender:%s Receiver:%s Time:\"%s %s\" Message:\"%s\"\n", nameOfSender, nameOfReceiver, time, clock, messageOfSender);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
        query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.rangeQuery("date").gte(date + "T" + "00:00:00.000").lte(date + "T" + "23:59:59.999"))
                .filter(QueryBuilders.termsQuery("sender", user))
                .filter(QueryBuilders.termsQuery("receiver", id));
        resp = client.prepareSearch("messages").setQuery(query).get();
        ress.clear();
        for (SearchHit searchHitFields : resp.getHits()) {
            ress.add(searchHitFields.getSourceAsString());
        }

        try {
            for (String s : ress) {
                int senderInd = s.indexOf("sender");
                int receiverInd = s.indexOf("receiver");
                int messageInd = s.indexOf("message");
                int dateInd = s.indexOf("date");
                String senderID = s.substring(senderInd + 9, receiverInd - 3);
                String receiverID = s.substring(receiverInd + 11, messageInd - 3);
                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                String time = s.substring(dateInd + 7, dateInd + 17);
                String clock = s.substring(dateInd + 18, dateInd + 26);
                PreparedStatement senderName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                senderName.setString(1, senderID);
                ResultSet resultset = null;
                resultset = senderName.executeQuery();
                String nameOfSender = null;
                while (resultset != null && resultset.next()) {
                    nameOfSender = resultset.getString("name");
                }

                if (nameOfSender == null) {
                    nameOfSender = senderID;
                }

                PreparedStatement receiverName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
                receiverName.setString(1, receiverID);
                resultset = null;
                resultset = receiverName.executeQuery();
                String nameOfReceiver = null;
                while (resultset != null && resultset.next()) {
                    nameOfReceiver = resultset.getString("name");
                }

                if (nameOfReceiver == null) {
                    nameOfReceiver = receiverID;
                }

                System.out.printf("Sender:%s Receiver:%s Time:\"%s %s\" Message:\"%s\"\n", nameOfSender, nameOfReceiver, time, clock, messageOfSender);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void view_messages_by_date_channel(String id, String date, Client client) {
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("id", id))
                .filter(QueryBuilders.rangeQuery("date").gte(date + "T" + "00:00:00.000").lte(date + "T" + "23:59:59.999"));
        SearchResponse resp = client.prepareSearch("messages3").setQuery(query).get();
        ArrayList<String> ress = new ArrayList<>();
        for (SearchHit searchHitFields : resp.getHits()) {
            ress.add(searchHitFields.getSourceAsString());
        }

        try {
            for (String s : ress) {
                int messageInd = s.indexOf("message");
                int dateInd = s.indexOf("date");
                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
                String time = s.substring(dateInd + 7, dateInd + 17);
                String clock = s.substring(dateInd + 18, dateInd + 26);
                System.out.printf("Time:\"%s %s\" Message:\"%s\"\n", time, clock, messageOfSender);
            }


        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
