import java.sql.PreparedStatement;

public class user_settings {
    String phoneNumber;
    String bio;
    DBConnection newConnection = new DBConnection();

    public user_settings(String phone_number, String bio) {
        this.phoneNumber = phone_number;
        this.bio = bio;
    }

    public user_settings(String phone_number) {
        this.phoneNumber = phone_number;
    }

    public void setBio(){
        newConnection.makeConnection();
        try {PreparedStatement setBioStmt = newConnection.connection.prepareStatement(" UPDATE user_settings SET bio=(?) where phone_number=(?)");
            setBioStmt.setString(2,phoneNumber);
            setBioStmt.setString(1,bio);
            setBioStmt.executeUpdate();
            newConnection.connection.close();
        }catch (Exception e){
            System.out.println(e);
        }
    }
    public void setSelfDestruct(int value){
       // System.out.println("setSelfDestructCalled");
       // System.out.println(phoneNumber);
        //String sql = "UPDATE user_settings SET self_destruct="+value+"WHERE phone_number= "+"'3456'";
        //String sql = " INSERT INTO user_settings (phoneNumber,self_destruct) values('"+phoneNumber +"','" + value + "')";
        try {
            newConnection.makeConnection();
           PreparedStatement preparedStatement= newConnection.connection.prepareStatement("UPDATE user_settings SET self_destruct=(?) WHERE phone_number=(?)");
            preparedStatement.setInt(1,value);
            preparedStatement.setString(2,phoneNumber);
            preparedStatement.executeUpdate();
            newConnection.connection.close();
        }catch (Exception e){
            System.out.println(e);
        }
    }
}
