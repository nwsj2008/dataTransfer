package edu.opinion.transfer.tesi;

import java.sql.Connection;
import java.sql.DriverManager;

public class TesiUtils {
	
	/**
	 * 获取特思的数据库连接
	 * @return
	 */
	public static Connection getTesiConnection(){
		
		String sDBDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		String sConnStr = "jdbc:sqlserver://" + TesiConfig.getInstance().TesiDataSource 
			+ ":1433;databaseName=" + TesiConfig.getInstance().TesiDatabase;
		String username = TesiConfig.getInstance().TesiUsername;
		String password = TesiConfig.getInstance().TesiPassword;
		Connection connection = null;

		try {
			Class.forName(sDBDriver);
			connection = DriverManager.getConnection(sConnStr, username,
					password);
		} catch (Exception e) {
			connection = null;
			e.printStackTrace();
		}
		return connection;
	}
	
	/**
	 * 获取系统的数据库连接
	 * @return
	 */
	public static Connection getSysConnection(){
		
		String sDBDriver = "com.mysql.jdbc.Driver";
		String sConnStr = "jdbc:mysql://" + TesiConfig.getInstance().SysDataSource 
			+ ":3306/" + TesiConfig.getInstance().SysDatabase;
		String username = TesiConfig.getInstance().SysUsername;
		String password = TesiConfig.getInstance().SysPassword;
		Connection connection = null;

		try {
			Class.forName(sDBDriver);
			connection = DriverManager.getConnection(sConnStr, username,
					password);
		} catch (Exception e) {
			connection = null;
			e.printStackTrace();
		}
		return connection;
	}
}
