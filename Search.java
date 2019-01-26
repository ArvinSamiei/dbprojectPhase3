import com.mysql.cj.protocol.Resultset;
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

public class Search {
    DBConnection dbConnection = new DBConnection();

    public void searchAll(String textToSearch, String sender, Client client) {
        dbConnection.makeConnection();
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("sender", sender))
                .filter(QueryBuilders.termsQuery("message", textToSearch));
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
//            PreparedStatement member = dbConnection.connection.prepareStatement("SELECT member_id FROM group_members " +
//                    "WHERE group_id=(?) AND member_id=(?)");
//            member.setString(1,groupId);
//            member.setString(2,phoneNumber);
//            resultSet=member.executeQuery();
//            if (resultSet.first()){
//                return true;
//            }


        } catch (Exception e) {
            e.printStackTrace();
        }

        query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("receiver", sender))
                .filter(QueryBuilders.termsQuery("message", textToSearch));
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
            System.out.println(e);
        }

    }

    public void search_spec(String textToSearch, String sender, String receiver, Client client) {
        dbConnection.makeConnection();
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("sender", sender))
                .filter(QueryBuilders.termsQuery("receiver", receiver))
                .filter(QueryBuilders.termsQuery("message", textToSearch));
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
                .filter(QueryBuilders.termsQuery("message", textToSearch));
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
//        BoolQueryBuilder query = QueryBuilders.boolQuery()
//                .filter(QueryBuilders.termsQuery("sender", sender))
//                .filter(QueryBuilders.termsQuery("message", textToSearch));
//        SearchResponse resp = client.prepareSearch("messages").setQuery(query).get();
//        ArrayList<String> ress = new ArrayList<String>();
//        for (SearchHit searchHitFields : resp.getHits()) {
//            ress.add(searchHitFields.getSourceAsString());
//        }
//
//        try {
//            for (String s : ress) {
//                int senderInd = s.indexOf("sender");
//                int receiverInd = s.indexOf("receiver");
//                int messageInd = s.indexOf("message");
//                int dateInd = s.indexOf("date");
//                String senderID = s.substring(senderInd + 9, receiverInd - 3);
//                String receiverID = s.substring(receiverInd + 11, messageInd - 3);
//                String messageOfSender = s.substring(messageInd + 10, dateInd - 3);
//                String time = s.substring(dateInd + 7, dateInd + 17);
//                String clock = s.substring(dateInd + 18, dateInd + 26);
//                PreparedStatement senderName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
//                senderName.setString(1, senderID);
//                ResultSet resultset = null;
//                resultset = senderName.executeQuery();
//                String nameOfSender = null;
//                while (resultset != null && resultset.next()) {
//                    nameOfSender = resultset.getString("name");
//                }
//
//                if (nameOfSender == null) {
//                    nameOfSender = sender;
//                }
//
//                PreparedStatement receiverName = dbConnection.connection.prepareStatement("select name from user where user_id=(?)");
//                receiverName.setString(1, receiverID);
//                resultset = null;
//                resultset = receiverName.executeQuery();
//                String nameOfReceiver = null;
//                while (resultset != null && resultset.next()) {
//                    nameOfReceiver = resultset.getString("name");
//                }
//
//                if (nameOfReceiver == null) {
//                    nameOfReceiver = receiverID;
//                }
//
//                System.out.printf("Sender:%s Receiver:%s Time:\"%s %s\" Message:\"%s\"\n", nameOfSender, nameOfReceiver, time, clock, messageOfSender);
//            }
////            PreparedStatement member = dbConnection.connection.prepareStatement("SELECT member_id FROM group_members " +
////                    "WHERE group_id=(?) AND member_id=(?)");
////            member.setString(1,groupId);
////            member.setString(2,phoneNumber);
////            resultSet=member.executeQuery();
////            if (resultSet.first()){
////                return true;
////            }
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    public void search_group_by_sender(String id, String sender, String textToSearch, Client client) {
        dbConnection.makeConnection();
        ArrayList<String> ress = new ArrayList<>();
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("id", id))
                .filter(QueryBuilders.termsQuery("sender", sender))
                .filter(QueryBuilders.termsQuery("message", textToSearch));
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
//            PreparedStatement member = dbConnection.connection.prepareStatement("SELECT member_id FROM group_members " +
//                    "WHERE group_id=(?) AND member_id=(?)");
//            member.setString(1,groupId);
//            member.setString(2,phoneNumber);
//            resultSet=member.executeQuery();
//            if (resultSet.first()){
//                return true;
//            }


        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
