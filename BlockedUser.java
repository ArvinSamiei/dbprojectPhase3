import java.sql.*;

public class BlockedUser {

   private DBConnection dbConnection= new DBConnection();


    public void addBlockedUser(String blocked , String blockedBy){
        dbConnection.makeConnection();
        try {
            PreparedStatement preparedStatement = dbConnection.connection.prepareStatement("INSERT INTO blocked_user VALUES (?,?)");
            preparedStatement.setString(1,blocked);
            preparedStatement.setString(2,blockedBy);
            preparedStatement.executeUpdate();
            dbConnection.connection.close();

        }catch (Exception e){
            System.out.println(e);
        }
    }
    public void unblockUser(String unblockedUser,String blockedBy){
        dbConnection.makeConnection();
        try{
            PreparedStatement preparedStatement = dbConnection.connection.prepareStatement("DELETE FROM blocked_user WHERE blocked_user=(?) " +
                    "AND blocked_by=(?)");
            preparedStatement.setString(1,unblockedUser);
            preparedStatement.setString(2,blockedBy);
            preparedStatement.executeUpdate();
            dbConnection.connection.close();
        }catch (Exception e){
            System.out.println(e);
        }

    }
}
