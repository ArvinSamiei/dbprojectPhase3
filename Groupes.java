import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Groupes {
    DBConnection dbConnection = new DBConnection();
    public void createGroup(String groupID , String name ,String adminPhoneNumber){
        dbConnection.makeConnection();
        try {
            PreparedStatement preparedStatement = dbConnection.connection.prepareStatement("INSERT INTO groupes (group_id,name,admin_phone_number)" +
                    "values (?,?,?)");
            preparedStatement.setString(1,groupID);
            preparedStatement.setString(2,name);
            preparedStatement.setString(3,adminPhoneNumber);
            preparedStatement.executeUpdate();
           dbConnection.connection.close();
        }catch (Exception e){
            System.out.println(e);
        }
    }
    public ResultSet viewGroupProfile(String groupID){
        ResultSet resultSet=null;
        dbConnection.makeConnection();
        try {
            PreparedStatement groupProfile = dbConnection.connection.prepareStatement(
                    "SELECT group_id,name,link FROM groupes WHERE group_id=(?) ");
            groupProfile.setString(1,groupID);
            resultSet = groupProfile.executeQuery();

        }catch (Exception e){
            System.out.println(e);
        }
        return resultSet;
    }
    public String findAdmin(String groupID) throws SQLException {
        dbConnection.makeConnection();
        ResultSet admin=null;
        try {
            PreparedStatement adminList = dbConnection.connection.prepareStatement("SELECT admin_phone_number FROM groupes WHERE group_id=(?)");
            adminList.setString(1,groupID);
            admin = adminList.executeQuery();
//            while (admin.next()){
//                System.out.println("While loop");
//                System.out.println(admin.getString("admin_phone_number"));
//            }

        }catch (Exception e){
            System.out.println(e);
        }
        admin.first();
        return admin.getString("admin_phone_number");
    }
    public void setLink(String groupID , String link){
        dbConnection.makeConnection();
        try {
            PreparedStatement setLink = dbConnection.connection.prepareStatement("UPDATE groupes SET link=(?) WHERE group_id=(?)");
            setLink.setString(1,link);
            setLink.setString(2,groupID);
            setLink.executeUpdate();


        }catch (Exception e){
            System.out.println(e);
        }
    }
    public String findGroupByLink(String link){
        dbConnection.makeConnection();
        String grouplID = null ;
        ResultSet groupWithSameLink=null;
        try {
            PreparedStatement ps =  dbConnection.connection.prepareStatement(
                    "SELECT group_id FROM groupes WHERE link=(?)");
            ps.setString(1,link);
            groupWithSameLink= ps.executeQuery();
            while (groupWithSameLink.next()){
                grouplID = groupWithSameLink.getString("group_id");
            }
            dbConnection.connection.close();

        }catch (Exception e){
            System.out.println(e);
        }
        return grouplID;
    }
}
