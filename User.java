import java.sql.*;
import java.time.LocalDateTime;

public class User {
   String phoneNumber;
   DBConnection connection = new DBConnection();

    public User(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }
    public void createUser(){
        String sql = "INSERT INTO user(phone_number) VALUES ('"+phoneNumber+"')";
        connection.insertRecord(sql);
    }
    public String password() {
        String checkUser = "SELECT phone_number,password FROM user WHERE phone_number = '"+phoneNumber+"'";
        connection.makeConnection();
        String password= null;
        try {
            ResultSet resultSet = connection.connection.createStatement().executeQuery(checkUser);
            while (resultSet.next()) {
                //System.out.println("while loop");
               // System.out.println(resultSet.getString("phone_number") + "\t" + resultSet.getString("password"));
                password = resultSet.getString("password");
            }
        }catch (Exception e){

        }

        return password;
    }
    public boolean existsUser(){
       // System.out.println(phoneNumber+"  "+phoneNumber.length());
        String checkUser = "SELECT phone_number,password FROM user WHERE phone_number = '"+phoneNumber+"'";
        connection.makeConnection();
        try {
            //System.out.println("try_block");
            ResultSet resultSet = connection.connection.createStatement().executeQuery(checkUser);
            //String password = null;
           // System.out.println(resultSet.getRow());
           /*while (resultSet.next()){
               System.out.println("while loop");
                System.out.println(resultSet.getString("phone_number") +"\t"+resultSet.getString("password"));
                password = resultSet.getString("password");
            }
            if (password !=null){
                System.out.println(password);
            }*/
            if(!resultSet.first()){
                System.out.println("IDisnotfound");
                connection.connection.close();
                return false;
            }
            else {
                //System.out.println("true returned");
                connection.connection.close();
                return true;
            }

        }catch (Exception e){
            System.out.println(e);

        }

        return false;
    }
    public void setPassword(String password){
        try {
            connection.makeConnection();
            PreparedStatement preparedStatement = connection.connection.prepareStatement(
                    "UPDATE user SET password=(?) WHERE phone_number=(?)");
            preparedStatement.setString(1,password);
            preparedStatement.setString(2,phoneNumber);
            preparedStatement.executeUpdate();
            connection.connection.close();

        }catch (Exception e){

        }
    }
    public void setName(String name){
        try {
            connection.makeConnection();
            PreparedStatement preparedStatement = connection.connection.prepareStatement(
                    "UPDATE user SET name=(?) WHERE phone_number=(?)");
            preparedStatement.setString(1,name);
            preparedStatement.setString(2,phoneNumber);
            preparedStatement.executeUpdate();
            connection.connection.close();
        }catch (Exception e){
            System.out.println(e);
        }

    }

    public ResultSet viewProfile(){
        ResultSet profile = null;
        try {
            connection.makeConnection();
            PreparedStatement query = connection.connection.prepareStatement("SELECT name,user_id,link FROM user" +
                    " WHERE phone_number=(?) ");
            query.setString(1,this.phoneNumber);
            profile = query.executeQuery();
        }catch (Exception e){
            System.out.println(e);
        }
        return profile;
    }
    public void updateLastLogin(String phoneNumber){
        connection.makeConnection();
        try {
            PreparedStatement updateLastLogin = connection.connection.prepareStatement(
                    "UPDATE user SET last_login=(?)WHERE user_id =(?)");
            updateLastLogin.setString(1,LocalDateTime.now().toString());
            updateLastLogin.setString(2,phoneNumber);
            updateLastLogin.executeUpdate();
            connection.connection.close();
        }catch (Exception e){
            System.out.println(e);
        }
    }


}

