package Stock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.lang.ObjectUtils.Null;
import org.json.JSONObject;

public class MySql {
	private static String url = "jdbc:mysql://127.0.0.1:3306/";
	// MySQL配置时的用户名
	private static String user = "root";
	// Java连接MySQL配置时的密码
	private static String password = "139444";
	private static Connection conn = null;

	/**
	 * Execute sql without return value
	 * 
	 * @param sql
	 * @param database
	 */
	public static void Execute(String sql, String database) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			// 连续数据库

			if (conn == null) {
				conn = DriverManager.getConnection(url + database
						+ "?useUnicode=true&characterEncoding=utf8", user,
						password);
			}
			if (conn.isClosed()) {
				System.out.println("DataBase closed!");
			}
			Statement statement = conn.createStatement();
			statement.execute(sql);
		} catch (SQLException e) {
			//System.out.println(sql);
			// e.printStackTrace();
		} catch (Exception e) {
			//System.out.println(sql);
			// e.printStackTrace();
		}
	}

	/**
	 * Execute sql with return value
	 * 
	 * @param sql
	 * @param database
	 * @return ResultSet
	 */
	public static ResultSet ExecuteQuery(String sql, String database) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			// 连续数据库
			Connection conn = DriverManager.getConnection(url + database, user,
					password);
			if (!conn.isClosed())
				System.out.println("Succeeded connecting to the Database!");
			// statement用来执行SQL语句
			Statement statement = conn.createStatement();
			statement.execute(sql);
			// 要执行的SQL语句
			ResultSet rs = statement.executeQuery(sql);
			String name = null;
			while (rs.next()) {
				// System.out.println(rs.getString("sno") + "\t" + name);
			}
			conn.close();
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Create table
	 * 
	 * @param database
	 * @param tableName
	 * @param keys
	 */
	public static void createTable(String database, String tableName,
			Set<String> keys) {
		String sql = "CREATE TABLE " + tableName
				+ " (id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY";
		for (String key : keys) {
			sql += "," + key + " TINYTEXT";
		}
		sql += ")ENGINE=InnoDB DEFAULT CHARSET=utf8";
		//System.out.println(sql);
		Execute(sql, database);
	}

	/**
	 * Create number table
	 * 
	 * @param database
	 * @param tableName
	 * @param NumberKeys
	 * @param StringKeys
	 */
	public static void createNumberTable(String database, String tableName,
			Set<String> NumberKeys, Set<String> StringKeys) {
		String sql = "CREATE TABLE " + tableName
				+ " (id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY";
		for (String key : NumberKeys) {
			sql += "," + key + " float";
		}

		for (String key : StringKeys) {
			sql += "," + key + " TINYTEXT";
		}

		sql += ")ENGINE=InnoDB DEFAULT CHARSET=utf8";
		// System.out.println(sql);
		Execute(sql, database);
	}

	/**
	 * Create number table
	 * 
	 * @param database
	 * @param tableName
	 * @param NumberKeys
	 * @param StringKey
	 */
	public static void createNumberTable(String database, String tableName,
			Set<String> NumberKeys, String StringKey) {
		String sql = "CREATE TABLE " + tableName
				+ " (id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY";
		for (String key : NumberKeys) {
			sql += "," + key + " float";
		}
		if (StringKey != null) {
			sql += "," + StringKey + " TINYTEXT";
		}

		sql += ")ENGINE=InnoDB DEFAULT CHARSET=utf8";
		// System.out.println(sql);
		Execute(sql, database);
	}

	/**
	 * Insert data Example:INSERT INTO table1(id, name, address) VALUES(1, ygl,
	 * 'beijing')
	 * 
	 * @param database
	 * @param tableName
	 * @param pairs
	 */
	public static void insertData(String database, String tableName,
			Map<String, String> pairs) {

		String keys = " (";
		String values = " (";
		for (Entry<String, String> pair : pairs.entrySet()) {
			keys += pair.getKey().trim() + ",";
			values += "'" + pair.getValue().trim() + "',";
		}
		keys = keys.substring(0, keys.length() - 1);
		values = values.substring(0, values.length() - 1);
		keys += " ) ";
		values += " );";
		String sql = "INSERT INTO " + tableName + keys + " VALUES " + values;
		// System.out.println(sql);
		Execute(sql, database);
	}

	/**
	 * Insert data
	 * 
	 * @param database
	 * @param tableName
	 * @param pairs
	 */
	public static void insertData(String database, String tableName,
			JSONObject pairs) {

		String keys = " (";
		String values = " (";
		Set<String> objectKeys = pairs.keySet();
		// System.out.println(objectKeys.size());
		try {
			for (String key : objectKeys) {
				keys += key.trim() + ",";
				values += "'" + pairs.getString(key).trim() + "',";
			}
			keys = keys.substring(0, keys.length() - 1);
			values = values.substring(0, values.length() - 1);
			keys += " ) ";
			values += " );";
			String sql = "INSERT INTO " + tableName + keys + " VALUES "
					+ values;
			// System.out.println(sql);
			Execute(sql, database);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	/**
	 * Insert data Example:INSERT INTO table1(id, name, address) VALUES(1, ygl,
	 * 'beijing')
	 * 
	 * @param database
	 * @param tableName
	 * @param pairs
	 */
	public static void insertNumberData(String database, String tableName,
			Map<String, Double> NumberPairs, Map<String, String> StrPairs) {

		String keys = " (";
		String values = " (";
		for (Entry<String, String> pair : StrPairs.entrySet()) {
			keys += pair.getKey().trim() + ",";
			values += "'" + pair.getValue().trim() + "',";
		}

		for (Entry<String, Double> pair : NumberPairs.entrySet()) {
			keys += pair.getKey().trim() + ",";
			if (Double.isInfinite(pair.getValue())
					|| Double.isNaN(pair.getValue())) {
				values += null + ",";
			} else {
				values += pair.getValue() + ",";
			}

		}

		keys = keys.substring(0, keys.length() - 1);
		values = values.substring(0, values.length() - 1);
		keys += " ) ";
		values += " );";
		String sql = "INSERT INTO " +  tableName + keys
				+ " VALUES " + values;
		//System.out.println(database);
		//System.out.println(sql);
		try {
			Execute(sql, database);
		} catch (Exception e) {
			System.out.println(sql);
			// TODO: handle exception
		}

	}

	public static boolean isNumeric(String str) {
		try {
			Float d = Float.valueOf(str);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}

}
