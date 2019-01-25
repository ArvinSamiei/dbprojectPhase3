import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;

public class ChannelMessage {
    DBConnection dbConnection =new DBConnection();
    public void sendMessage(String channelID , String text){
        dbConnection.makeConnection();
        try {
            PreparedStatement ps = dbConnection.connection.prepareStatement("INSERT INTO channel_messages(channel_id , text,date)" +
                    " values ( ?,?,?)");
            ps.setString(1,channelID);
            ps.setString(2,text);
            ps.setString(3,LocalDateTime.now().toString());
            ps.executeUpdate();
            dbConnection.connection.close();
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
