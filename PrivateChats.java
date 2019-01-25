import javax.print.DocFlavor;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;


public class PrivateChats {
    DBConnection dbConnection = new DBConnection();

    public void sendMessage(String sender, String receiver, String text) {
        dbConnection.makeConnection();
        try {
            PreparedStatement preparedStatement = dbConnection.connection.prepareStatement(" INSERT  INTO private_chats (sender,receiver,text,state,date)" +
                    "VALUES ((?),(?),(?),0,?)");
            preparedStatement.setString(1,sender);
            preparedStatement.setString(2,receiver);
            preparedStatement.setString(3,text);
            preparedStatement.setString(4,LocalDateTime.now().toString());
            preparedStatement.executeUpdate();
            dbConnection.connection.close();

        } catch (Exception e) {
            System.out.println(e);

        }
    }
    public ResultSet fetchMessages(String phoneNumber){
        dbConnection.makeConnection();
        ResultSet resultSet=null;
        try {
            PreparedStatement ps = dbConnection.connection.prepareStatement("SELECT sender,receiver,text FROM private_chats" +
                    " WHERE sender= (?) OR receiver=(?) LIMIT 20");
            ps.setString(1,phoneNumber);
            ps.setString(2,phoneNumber);
            resultSet=ps.executeQuery();
//            while (resultSet.next()){
//                System.out.println(resultSet.getString("sender")+"\t"+resultSet.getString("receiver")+
//                resultSet.getString("text"));
//            }
        }catch (Exception e){
            System.out.println(e);
        }
        return resultSet;
    }
    public int countUnreadMessages(String phoneNumber){
        int count = -1;
        dbConnection.makeConnection();
        try {
            PreparedStatement findCountOfUnreadMessages = dbConnection.connection.prepareStatement("SELECT COUNT(*) AS count FROM private_chats " +
                    "WHERE receiver=(?) and state=0");
            findCountOfUnreadMessages.setString(1,phoneNumber);
            ResultSet resultSet = findCountOfUnreadMessages.executeQuery();
            resultSet.next();
            count = resultSet.getInt("count");

        }catch (Exception e){
            System.out.println(e);
        }
        return count;
    }


}