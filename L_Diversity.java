
import java.awt.List;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Date;
import java.util.HashSet;

import com.mysql.cj.protocol.Resultset;

public class L_Diversity {
	
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:/test";
	
	static final String USER = "root";
	static final String PASS = "";
	static ResultSet resultset = null;
	static ResultSet resultsett = null;
	static ResultSet resultsettt = null;
	static ResultSet resultset2 = null;
	static ResultSet resultset3 = null;
	static ResultSet resultsettemp = null;
	static ResultSetMetaData rsmd = null;
	static PreparedStatement preparedstmt = null;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Connection conn = null;
		Statement stmt = null;
		Statement stmtt = null;
		Statement stmttt = null;
		int number_of_groups=0;
		Date date = new Date();
		String starting_time = date.toString();

		long startTime = System.nanoTime();
		
			try {
				conn = DriverManager.getConnection(DB_URL,USER,PASS);
				//int i=1;
				//F-1 Code for making TPDT
	
				stmt = conn.createStatement();
				stmtt = conn.createStatement();
				stmttt = conn.createStatement();
				
					
				//------Making	TPDT' ----------------\\				
				
				resultset = (ResultSet) stmt.executeQuery("Select * from transformed2");
				
				int row=1; int gidd=1; int counter=0;
				 
				while(resultset.next())
				{
					System.out.println(gidd);
					
					if(resultset.getInt(8)==0)
					{
						
						String insertintpdt = "UPDATE transformed2 set RID=? WHERE DUID ='" + resultset.getString(1) + "'";
						preparedstmt = conn.prepareStatement(insertintpdt);
						preparedstmt.setInt(1, row);
						preparedstmt.execute();		//******************
						row=row+1;
						
						String insertion2 = "update transformed2 set gid=? where duid ='" + resultset.getString(1) + "'";
						PreparedStatement preparedstmt = conn.prepareStatement(insertion2);
						preparedstmt.setString(1, Integer.toString(gidd));
						preparedstmt.execute();		//******************
					}
					
					counter=counter+1;
					
					if(counter % 5 == 0)		//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX1
					{
						
						gidd=gidd+1;
						counter=0;
					}
					
					
					//System.out.print(resultset.getString(3));
					//System.out.println();
				}
				resultset.close();
				
				
				int total_groups=gidd;
				
				int RID_RS1=0;
				int DUID=0;
				String DOBMM="";
				String DOBYY="";
				String RACE="";
				String EDUY="";
				String INCOME="";
				int CODE=0;
				int GID=0;
				int RID=0;
				
				ArrayList<Integer> tempcode = new ArrayList<Integer>();
				
				for(int i=1;i<=total_groups;i++)
				{
					resultset = (ResultSet) stmt.executeQuery("Select count(distinct CODE), DUID, DOBMM, DOBYY, RACE, EDUY, INCOME, CODE, GID, RID from transformed2 where GID = '" +i+"'");
					resultset.next();
					RID_RS1=resultset.getInt(10);
					DUID=resultset.getInt(2); DOBMM=resultset.getString(3); DOBYY=resultset.getString(4); 
					RACE=resultset.getString(5); EDUY=resultset.getString(6); INCOME=resultset.getString(7);
					CODE=resultset.getInt(8);
					if(resultset.getInt(1)<3)
					{
						System.out.println("Group " + i + "has problem");
						resultsett = (ResultSet) stmt.executeQuery("Select distinct CODE from transformed2 where GID = '"+i+"'");
						while(resultsett.next())
						{
							tempcode.add(resultsett.getInt(1));
						}
						resultsett.close();
						
						if(tempcode.size()!=0)
						{
							System.out.println("Problem Value " + tempcode.get(0) + " - " + tempcode.get(1));
						
							resultsettt = (ResultSet) stmt.executeQuery("Select * from transformed2 where CODE <> '"+tempcode.get(0)+"' and CODE <> '"+tempcode.get(1)+"' and GID <> '"+i+"'");
							resultsettt.next();
						
						
							tempcode.clear();
						
							String insertintpdt = "UPDATE transformed2 set DUID=?, DOBMM=?, DOBYY=?, RACE=?, EDUY=?, INCOME=?, CODE=? WHERE RID ='" + RID_RS1 + "'";
							preparedstmt = conn.prepareStatement(insertintpdt);
							preparedstmt.setInt(1, resultsettt.getInt(1));			preparedstmt.setString(5, resultsettt.getString(5));
							preparedstmt.setString(2, resultsettt.getString(2));	preparedstmt.setInt(6, resultsettt.getInt(6));
							preparedstmt.setString(3, resultsettt.getString(3));	preparedstmt.setInt(7, resultsettt.getInt(7));
							preparedstmt.setString(4, resultsettt.getString(4));	
							preparedstmt.execute();		//******************
						
							insertintpdt = "UPDATE transformed2 set DUID=?, DOBMM=?, DOBYY=?, RACE=?, EDUY=?, INCOME=?, CODE=? WHERE RID ='" + resultsettt.getString(9) + "'";
							preparedstmt = conn.prepareStatement(insertintpdt);
							preparedstmt.setInt(1, DUID);			preparedstmt.setString(5, EDUY);
							preparedstmt.setString(2, DOBMM);	preparedstmt.setString(6, INCOME);
							preparedstmt.setString(3, DOBYY);	preparedstmt.setInt(7, CODE);
							preparedstmt.setString(4, RACE);	
							preparedstmt.execute();		//******************
						
						
							resultsettt.close();
						
						}
					}
					resultset.close();
					
				}
				
				
			
				
				//-----------------Anonymization--------------
				
				int num_of_groups=gidd; int group_id=1;
				
				while(group_id <= num_of_groups)
				{
					resultset = (ResultSet) stmt.executeQuery("Select min(cast(DOBMM AS SIGNED)), max(cast(DOBMM AS SIGNED)), min(cast(DOBYY AS SIGNED)), max(cast(DOBYY AS SIGNED)), min(cast(EDUY AS SIGNED)), max(cast(EDUY AS SIGNED)), min(cast(INCOME AS SIGNED)), max(cast(INCOME AS SIGNED) ) from transformed2 where gid = '" + group_id +"'");
					
					while(resultset.next())
					{			
						System.out.println("gg");
						String colvalue1,colvalue2,colvalue3,colvalue4,colvalue5,colvalue6,colvalue7,colvalue8 = "0";
						
						colvalue1 = resultset.getString(1); System.out.print(" Min DOBMM : " + colvalue1 + " ");
						colvalue2 = resultset.getString(2); System.out.print(" Max DOBMM : " + colvalue2 + " ");
						colvalue3 = resultset.getString(3); System.out.print(" Min DOBYY : " + colvalue3 + " ");
						colvalue4 = resultset.getString(4); System.out.print(" Max DOBYY : " + colvalue4 + " ");
						colvalue5 = resultset.getString(5); System.out.print(" Min EDUY : " + colvalue5 + " ");
						colvalue6 = resultset.getString(6); System.out.print(" Max EDUY : " + colvalue6 + " ");
						colvalue7 = resultset.getString(7); System.out.print(" Min INCOME : " + colvalue5 + " ");
						colvalue8 = resultset.getString(8); System.out.print(" Max INCOME : " + colvalue6 + " ");
						
						
						String insertion2 = "update transformed2 set DOBMM=?, DOBYY=?, EDUY=?, INCOME=? where gid ='" + group_id + "'";
						PreparedStatement preparedstmt = conn.prepareStatement(insertion2);
						preparedstmt.setString(1, colvalue1+"_"+colvalue2);
						preparedstmt.setString(2, colvalue3+"_"+colvalue4);
						preparedstmt.setString(3, colvalue5+"_"+colvalue6);
						preparedstmt.setString(4, colvalue7+"_"+colvalue8);
						
						preparedstmt.execute();
						
						
//						System.out.println("Handling Row # " + row_number);
//						
//						row_number = row_number + 1;
					}
					
					//System.out.println("----------------------------------------------");
					
					group_id = group_id + 1;
					
				}//while loop brace
				
				//-----------------------Anonymization Done------------------------------
				
				//Extending Groups
				
//				resultset = (ResultSet) stmt.executeQuery("Select * from transformed where DUID=20017");
//				
//				while(resultset.next())
//				{
//					resultset2 = (ResultSet) stmtt.executeQuery("Select * from transformed2 where RIGHT(DOBMM, InStr(DOBMM,'_'))< '" + resultset.getInt(2) + "'");
//					//resultset2 = (ResultSet) stmtt.executeQuery("Select RIGHT(DOBMM, inStr(DOBMM,'_')) from transformed2");
//					//resultset2 = (ResultSet) stmtt.executeQuery("Select * from transformed2 where RIGHT(DOBMM, inStr(DOBMM,'_') < '" + resultset.getString(2) + "'" );
//					while(resultset2.next())
//					{
//						System.out.println("DOBMM = " + resultset2.getString(2) + " GID = " + resultset2.getString(8));
//						//System.out.println("DOBMM = " + resultset2.getString(1));
//					}
//				}
				
				//Extending Groups Done
				
					//--------Everything Done---------------\\
					
				
				
					
					//-------------------------------------------------\\
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//F-1 -------------
		System.out.println("Hello");
	}

}
