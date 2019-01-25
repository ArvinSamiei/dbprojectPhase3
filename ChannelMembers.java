import java.rmi.server.ExportException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;

public class ChannelMembers {
    DBConnection dbConnection = new DBConnection();
    public void addMember(String phoneNumber , String channelID){
        dbConnection.makeConnection();
        try {
            PreparedStatement ps = dbConnection.connection.prepareStatement("INSERT INTO channel_members(channel_id,member_id,last_seen) " +
                    "VALUES (?,?,?)");
            ps.setString(1,channelID);
            ps.setString(2,phoneNumber);
            ps.setString(3,LocalDateTime.now().toString());
            ps.executeUpdate();
            dbConnection.connection.close();
        }catch (Exception e){
            System.out.println(e);
        }

    }
    public void removeMember(String memberPhoneNumber , String channelID){
        dbConnection.makeConnection();
        try {
            PreparedStatement ps = dbConnection.connection.prepareStatement("DELETE  FROM channel_members WHERE channel_id=(?) AND member_id=(?)");
            ps.setString(1,channelID);
            ps.setString(2,memberPhoneNumber);
            ps.executeUpdate();
            dbConnection.connection.close();
        }catch (Exception e){
            System.out.println(e);
        }
    }
    public ResultSet viewMembers(String channelID){
        ResultSet resultSet=null;
        dbConnection.makeConnection();
        try {
            PreparedStatement preparedStatement = dbConnection.connection.prepareStatement(
                    "SELECT member_id FROM channel_members,user where channel_id=(?) AND user_id=member_id ORDER BY last_login");
            preparedStatement.setString(1,channelID);
            resultSet = preparedStatement.executeQuery();


        }catch (Exception e){
            System.out.println(e);
        }
        return resultSet;
    }
}
