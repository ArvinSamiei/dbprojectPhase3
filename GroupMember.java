import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;

public class GroupMember {
    DBConnection dbConnection = new DBConnection();
    public void removeMember(String groupID,String memberID){
        dbConnection.makeConnection();
        try {
            PreparedStatement preparedStatement = dbConnection.connection.prepareStatement("DELETE FROM group_members WHERE " +
                    "group_id=(?)AND member_id=(?)");
            preparedStatement.setString(1,groupID);
            preparedStatement.setString(2,memberID);
            preparedStatement.executeUpdate();
            dbConnection.connection.close();

        }catch (Exception e){
            System.out.println(e);
        }


    }
    public boolean isMember(String groupId,String phoneNumber){
        dbConnection. makeConnection();
        ResultSet resultSet=null;
        try {
            PreparedStatement member = dbConnection.connection.prepareStatement("SELECT member_id FROM group_members " +
                    "WHERE group_id=(?) AND member_id=(?)");
            member.setString(1,groupId);
            member.setString(2,phoneNumber);
            resultSet=member.executeQuery();
            if (resultSet.first()){
                return true;
            }



        }catch (Exception e){
            System.out.println(e);
        }
        return false;
    }
    public void addMember(String groupID,String phoneNumber){
        dbConnection.makeConnection();
        try {
            PreparedStatement ps = dbConnection.connection.prepareStatement(
                    "INSERT INTO group_members (group_id,member_id,last_seen)VALUES (?,?,?)");
            ps.setString(1,groupID);
            ps.setString(2,phoneNumber);
            ps.setString(3,LocalDateTime.now().toString());
            ps.executeUpdate();
            dbConnection.connection.close();

        }catch (Exception e){
            System.out.println(e);
        }
    }
}
