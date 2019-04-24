package dataPreDeal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MysqlDB {	
    //声明Connection对象
    Connection con;
   // 创建statement类对象，用来执行SQL语句！！
    Statement statement;   
    ResultSet rs1;
    Boolean rs2;
	public MysqlDB() {      
        try {
        	 //加载驱动程序
			Class.forName("com.mysql.jdbc.Driver");
			try {
				//1.getConnection()方法，连接MySQL数据库！！
				con = DriverManager.getConnection("jdbc:mysql://localhost:3366/test","root","123");
			} catch (SQLException e) {
				// TODO Auto-generated catch block 
				e.printStackTrace();
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public ResultSet exetuteColumn(String sql){	
		
		 //2.创建statement类对象，用来执行SQL语句！！		
		 try {
			statement = con.createStatement();
            //3.ResultSet类，用来存放获取的结果集！！
			 rs1 = statement.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return rs1;
	}
	
	public boolean exetute(String sql){
		 //2.创建statement类对象，用来执行SQL语句！！
		 boolean rs2=false;
		 try {
			statement = con.createStatement();
           //3.ResultSet类，用来存放获取的结果集！！
			 rs2 = statement.execute(sql);			 
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return rs2;
	}
	
	/**
	 * 关闭结果集ResultSet和Connection对象
	 */
	public void close(){
        try {
			rs1.close();			
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}       			
	}	
		
}
	 

	

