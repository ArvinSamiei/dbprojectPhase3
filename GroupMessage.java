import java.sql.PreparedStatement;
import java.time.LocalDateTime;


public class GroupMessage {
    DBConnection dbConnection = new DBConnection();
    public void sendMessage(String groupID,String memberPhoneNumber,String text){
        dbConnection.makeConnection();
        try {
            PreparedStatement sendMessage = dbConnection.connection.prepareStatement("INSERT INTO group_messages(group_id,text,sender_id,date) VALUES (?,?,?,?)");
            sendMessage.setString(1,groupID);
            sendMessage.setString(2,text);
            sendMessage.setString(3,memberPhoneNumber);
            sendMessage.setString(4,LocalDateTime.now().toString());
            sendMessage.executeUpdate();
            dbConnection.connection.close();

        }catch (Exception e){
            System.out.println(e);
        }
    }
}
