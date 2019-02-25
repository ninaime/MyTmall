package com.farm.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class DBUtils {
	private static String DRIVER;//数据库连接驱动
	private static String URL;//数据库连接路径
	private static String USERNAME;//数据库用户名
	private static String PASSWORD;//数据库密码
	static {
		//因为操作属性文件，所以java语言提供了专门的类来做这件事
		Properties properties = new Properties();
		try {
			//加载文件
			InputStream in = DBUtils.class.getClassLoader().getResourceAsStream("config/datasource.properties");
			properties.load(in);//加载配置文件
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		DRIVER = properties.getProperty("driver");
		URL = properties.getProperty("url");
		USERNAME = properties.getProperty("username");
		PASSWORD = properties.getProperty("password");
		try {
			Class.forName(DRIVER);//加载驱动，放在静态初始化块中，只执行一次
			System.out.println("加载驱动成功");
		} catch (ClassNotFoundException e) {
			System.out.println("注册JDBC驱动异常！");
		}
		
		
	}
	
	/**
	 * 1.注册驱动
	 * 2.编写URL
	 * 3.打开Connection连接
	 * 4.
	 */
	
	
	/**
	 * openConnection 开启数据库连接
	 * @exception 异常已在此处处理
	 * @return 没有返回值
	 */
	public static Connection getConnection(){
		Connection conn = null;
		try {
			System.out.println(URL+"  "+ USERNAME+"  "+PASSWORD);
			//打开connection连接
			conn = DriverManager.getConnection(URL,USERNAME,PASSWORD);
			System.out.println("数据库Connection连接成功！");
		}  catch (SQLException e) {
			System.out.println("数据库Connection连接失败！");
		}
		return conn;//连接成功后，返回conn连接给方法的调用者
	}
	
	/**
	 * closeAll 关闭所有连接
	 * @param ResultSet 结果集对象
	 * @param PreparedStatement 数据库操作对象
	 * @param Connection 数据库连接对象
	 * @return 没有返回值
	 */
	public static void closeAll(Connection conn,Statement stmt,ResultSet rs) {
		// 7、关闭对象，回收数据库资源  
        if (rs != null) { //关闭结果集对象  
            try {  
                rs.close();  
            } catch (SQLException e) {  
                e.printStackTrace();  
            }  
        }  
        if (stmt != null) { // 关闭数据库操作对象  
            try {  
                stmt.close();  
            } catch (SQLException e) {  
                e.printStackTrace();  
            }  
        }  
        if (conn != null) { // 关闭数据库连接对象  
            try {  
                if (!conn.isClosed()) {  
                    conn.close();  
                }  
            } catch (SQLException e) {  
                e.printStackTrace();  
            }  
        }  
	}
	
	/**
	 * DML DML操作集封装
	 * @exception pstmt = conn.prepareStatement(sql)会抛出空指针异常
	 * @param sql SQL语句
	 * @param conditionMap SQL语句中问号的代参
	 * @return 返回数据库受影响的行数
	 */
	public static boolean update(String sql,List<Object> parameter){
		int result =0;
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			//获取数据库连接
			conn = DBUtils.getConnection();
			//开启事物
			conn.setAutoCommit(false);
			//获取数据库操作对象
			stmt = conn.prepareStatement(sql);
			//判断传进来的参数是否为空，如果为空则不需要添加参数
	    	if(parameter!=null&&parameter.size()!=0){
	    		//遍历参数
	    		for(int i=0;i<parameter.size();i++){
	    			stmt.setObject(i+1, parameter.get(i));
	    		}
	    	}
	    	System.out.println(sql);
			result = stmt.executeUpdate();
			conn.commit();
		} catch (SQLException e) {
			System.out.println("数据连接异常！");
			e.printStackTrace();
		}  catch (NullPointerException e) {
			e.printStackTrace();
			System.out.println("数据操作异常！");
		} finally {
			DBUtils.closeAll(conn, stmt, null);
		}
		return result>0?true:false;
	}
	
	/**
	 * @param sql SQL语句
	 * @param conditionMap SQL语句中问号的代参
	 * @return 返回查询多条的数据
	 */
	public static List<Map<String, Object>> query(String sql,List<Object> parameter){
		//定义数据库连接对象
		Connection conn = null;
	    PreparedStatement stmt = null;
	    ResultSet rs = null;
	    //定义返回值容器
	    List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
	    try {
	    	//打开数据库连接
	    	conn = getConnection();
	    	//获取数据库操作对象
	    	stmt = conn.prepareStatement(sql);
	    	//判断传进来的参数是否为空，如果为空则不需要添加参数
	    	if(parameter!=null&&parameter.size()!=0){
	    		//遍历参数
	    		for(int i=0;i<parameter.size();i++){
	    			//接收参数的值
	    			Object paramValue = parameter.get(i);
	    			//判断值是否为Integer类型
	    			int j=i+1;
	    			if("java.lang.Integer".equalsIgnoreCase(paramValue.getClass().getName())){
	    				//如果是则将参数赋值
	    				stmt.setInt(j, Integer.parseInt(paramValue.toString()));
	    			}else if("java.lang.String".equalsIgnoreCase(paramValue.getClass().getName())){
	    				stmt.setString(j, paramValue.toString());
	    			}
	    			//此处应该再加一个date判断
	    		}
	    	}
	    	System.out.println(sql);
	    	rs = stmt.executeQuery();
	    	//接收返回值
	    	ResultSetMetaData rsmd = rs.getMetaData();
	    	while(rs.next()){
	    		Map<String, Object> dataMap = new HashMap<String, Object>(0);
	    		for(int i=1;i<=rsmd.getColumnCount();i++){
	    			dataMap.put(rsmd.getColumnName(i), rs.getObject(i));
	    		}
	    		resultList.add(dataMap);
	    	}
	    } catch (SQLException e) {
	    	e.printStackTrace();
	    	System.out.println("数据连接异常！");
	    } catch (NullPointerException e) {
	    	e.printStackTrace();
	    	System.out.println("数据操作异常！");
	    } finally{
	    	closeAll(conn, stmt, rs);
	  }
	  return resultList;
	}
	
	
	public static int getCount(List<Map<String, Object>> resultList) {
		Object count = resultList.get(0).get("COUNT");
		if(count==null) {
			return -1;
		}else {
			return Integer.valueOf(count.toString());
		}
	}
}
