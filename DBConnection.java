
import java.sql.*;


public class DBConnection {
     Connection connection;
   public void makeConnection(){
       try {
           Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(
                   "jdbc:mysql://localhost:3306/database?autoReconnect=true&useSSL=false",
                   "root","1135971374");

       }catch (Exception e){
           System.out.println(e);
       }
   }

   public void insertRecord(String insertCommand){
      makeConnection();
      try {
          connection.createStatement().executeUpdate(insertCommand);
          connection.close();
      }catch (Exception e){
          System.out.println(e);
      }
   }
   public ResultSet findRecord(String findCommand){
       System.out.println("findRecord executed");
       ResultSet resultSet=null;
       try {
             resultSet = connection.createStatement().executeQuery(findCommand);
             connection.close();
       }catch (Exception e){
           System.out.println(e);
       }
       return resultSet;
   }
}

