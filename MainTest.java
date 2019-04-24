package dataPreDeal;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class MainTest{
	public static void main(String[] args) {		
		MysqlDB db=new MysqlDB(); 
    	//String tablename2="fssj2_table_copy";
		/*int i1=1;
		int i2=10;
		String designtablename1="fssj1_table_copy_1";
    	String designtablename2="fssj2_table_copy_2";
    	int length1=9;
   		int length2=10;
   	    designTable(designtablename1,length1,db,i1);
    	designTable(designtablename2,length2,db,i2);
		 */
    	//dealXqAndSj(tablename1, db);
    	//dealXqAndSj(tablename2,db);
    	//String tablename1="fssj1_table_copy";
	//	String deleteablename1="fssj1_table_copy";
    //	String deletetablename2="fssj2_table_copy";
		deleteSamePersonRecord("isShangke1_test", db,1);	
    	deleteSamePersonRecord("isShangke2_test", db,2);
    	deleteSamePersonRecord("isShangke3_test", db,3);
    	deleteSamePersonRecord("isShangke4_test", db,4);
    	deleteSamePersonRecord("isShangke5_test", db,5);
    	deleteSamePersonRecord("isShangke6_test", db,6);
    	deleteSamePersonRecord("isShangke7_test", db,7);
    	deleteSamePersonRecord("isShangke8_test", db,8);
    	deleteSamePersonRecord("isShangke9_test", db,9);
    	deleteSamePersonRecord("isShangke10_test", db,10);
    	deleteSamePersonRecord("isShangke11_test", db,11);
    	deleteSamePersonRecord("isShangke12_test", db,12);
    	deleteSamePersonRecord("isShangke13_test", db,13);
    	deleteSamePersonRecord("isShangke14_test", db,14);
    	deleteSamePersonRecord("isShangke15_test", db,15);
    	deleteSamePersonRecord("isShangke16_test", db,16);
    	deleteSamePersonRecord("isShangke17_test", db,17);
    	deleteSamePersonRecord("isShangke18_test", db,18);
    	deleteSamePersonRecord("isShangke19_test", db,19);
    	//deleteSamePersonRecord(deletetablename2,db);
		/*String designtablename1="fssj1_table_copy";
    	String designtablename2="fssj2_table_copy";*/
		
    
    	db.close();
    	
   }
	
	/**
	 * 设计表结构，添加列
	 * @param tablename1
	 * @param length
	 * @param db
	 */
	private static void designTable(String tablename, int length, MysqlDB db,int i) {	
		for(int j=i;j<i+length;j++){
			String sql="alter table "+tablename+" add column isShangke"+j+" int(1)";
			db.exetute(sql);
		}
		System.out.println(tablename+"添加完成");
	}
	
	/**
	 * 删除同一个人在10分钟之内的消费数据
	 * a表示第几门课
	 */
	private static void deleteSamePersonRecord(String tablename,MysqlDB db,int a) {		
		String sql="select id,zh,time,isShangke"+a+" from "+tablename+"";
		ResultSet rs=db.exetuteColumn(sql);	
		int i=0;//计数器
		//字段值
		int id;
		int zh;
		long time;
		int isShangke;
		
		int pre = 0;//前一个账号
		int preisSk =-1;//前一个isShangke
		int currisSk=-1;//当前isShangke
		int curr;//当前账号
		long pretime = 0;//前一个时间
		long currtime;//当前时间
		List<Integer> list=new ArrayList<Integer>();
		try {
			while(rs.next()){
				id=rs.getInt(1);
				zh=rs.getInt(2);
				time =rs.getLong(3);
				isShangke=rs.getInt(4);			
				if(i==0){
					pre=zh;
					curr=zh;
					pretime=time;
					currtime=time;
					preisSk=isShangke;
					currisSk=isShangke;
				}else{
					curr=zh;
					currtime=time;
					currisSk=isShangke;
					//判断是否是同一个人而且在10分钟以内
					if((curr==pre)&&(currtime-pretime<=10)&&(currisSk==1 && preisSk==1)){
						list.add(id);
					}
					pre=curr;
					pretime=currtime;
					preisSk=currisSk;
				}				
				i++;	
				
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		//删除记录
		deleteRecord(list,db,tablename);	
		System.out.println(tablename+" done！");	
	}
	

	/**
	 * 删除数据库中不符合条件的记录
	 * @param list
	 * @param db 
	 * @param tablename 
	 */
	private static void deleteRecord(List<Integer> list, MysqlDB db, String tablename) {
		for(int i=0;i<list.size();i++){
			String sql="delete from "+tablename+" where id="+list.get(i);
			db.exetute(sql);
		}		
			
	}

	/**
	 * 更新日期和星期，把时间变成数字
	 * @param tablename
	 */
	public static void dealXqAndSj(String tablename,MysqlDB db){      	
        //要执行的SQL语句
		String sql = "select id,fssj from "+tablename+"";      
		ResultSet rs= db.exetuteColumn(sql);		
		try {
			while(rs.next()){
				int id=rs.getInt(1);
				//获得日期，为后面的星期几做准备
				Date fssj=rs.getDate(2);
				int xq=fssj.getDay();
				//获得时分
				Timestamp ts=rs.getTimestamp(2);	
				long time=ts.getTime()/(1000*60);//单位是分钟
			    SimpleDateFormat formattime = new SimpleDateFormat("HH:mm");
			    String sj=formattime.format(ts);				 
				//updateXqAndSj(db,id,xq,sj,tablename);	
				//updateDate(fssj,db,id,tablename);
			    updateTimeToLong(db,id,tablename,time);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
     
       System.out.println(tablename+"更新完成");        
}

	/**
	 * 将时间变为数字
	 * @param db
	 * @param id
	 * @param tablename
	 * @param time
	 */
	private static void updateTimeToLong(MysqlDB db, int id, String tablename, long time) {
		String sql1="update "+tablename+" set time="+time+" where id="+id;
		db.exetute(sql1);		
	}

	/**
	 * 获得年月日
	 * @param fssj
	 * @param db
	 * @param id
	 * @param tablename
	 */
	private static void updateDate(Date fssj, MysqlDB db, int id, String tablename) {
		String sql1="update "+tablename+" set ymd='"+fssj+"' where id="+id;
		db.exetute(sql1);
	}
	
	/**
	 * 获得星期和时分秒
	 * @param db
	 * @param id
	 * @param xq
	 * @param sj
	 * @param tablename
	 */
	private static void updateXqAndSj(MysqlDB db, int id, int xq, String sj,String tablename) {
		//更新星期
		String sql1="update "+tablename+" set xq="+xq+" where id="+id;
		db.exetute(sql1);
		//更新时间
		String sql2="update "+tablename+" set sf='"+sj+"' where id="+id;//注意日期是字符串
		db.exetute(sql2);
	}		

}
