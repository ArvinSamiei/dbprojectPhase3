import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ChannelMessage {
    DBConnection dbConnection =new DBConnection();
    public void sendMessage(String channelID , String text, Client client, long channel_counter_id){
        dbConnection.makeConnection();
        String date = LocalDateTime.now().toString();
        try {
            PreparedStatement ps = dbConnection.connection.prepareStatement("INSERT INTO channel_messages(channel_id , text,date)" +
                    " values ( ?,?,?)");
            ps.setString(1,channelID);
            ps.setString(2,text);
            ps.setString(3,date);
            ps.executeUpdate();
            dbConnection.connection.close();

            try {
                IndexResponse response = client.prepareIndex("messages3", "channel", Long.toString(++channel_counter_id))
                        .setSource(jsonBuilder()
                                .startObject()
                                .field("id", channelID)
                                .field("message", text)
                                .field("date", date)
                                .endObject()
                        )
                        .execute().actionGet();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }catch (Exception e){
            System.out.println(e);

        }
    }
    public ResultSet fetchMessages(String channelID){
        dbConnection.makeConnection();
        ResultSet resultSet=null;
        try {
            PreparedStatement ps= dbConnection.connection.prepareStatement("SELECT text  FROM channel_messages WHERE channel_id = (?)  ORDER BY date LIMIT 20 ");
            ps.setString(1,channelID);
            resultSet=ps.executeQuery();

        }catch (Exception e){
            System.out.println(e);
        }
        return resultSet;
    }

}
