import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class GroupMessage {
    DBConnection dbConnection = new DBConnection();

    public void sendMessage(String groupID, String memberPhoneNumber, String text, Client client, long group_counter_id) {
        dbConnection.makeConnection();
        String date = LocalDateTime.now().toString();
        try {
            PreparedStatement sendMessage = dbConnection.connection.prepareStatement("INSERT INTO group_messages(group_id,text,sender_id,date) VALUES (?,?,?,?)");
            sendMessage.setString(1, groupID);
            sendMessage.setString(2, text);
            sendMessage.setString(3, memberPhoneNumber);
            sendMessage.setString(4, date);
            sendMessage.executeUpdate();
            dbConnection.connection.close();
            try {
                IndexResponse response = client.prepareIndex("messages2", "group", Long.toString(++group_counter_id))
                        .setSource(jsonBuilder()
                                .startObject()
                                .field("id", groupID)
                                .field("sender", memberPhoneNumber)
                                .field("message", text)
                                .field("date", date)
                                .endObject()
                        )
                        .execute().actionGet();
                System.out.println(group_counter_id);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
