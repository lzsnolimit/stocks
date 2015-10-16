package Stock;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Zhongshan Lu
 **/

public class Hbase {
	public static Configuration configuration = HBaseConfiguration.create();
	public static String tableName = "";
	public static String familyname = "";

	public static void setStrings(String tableStr, String familyStr) {
		tableName = tableStr;
		familyname = familyStr;
	}

	/**
	 * Create a table
	 * 
	 * @param boollen
	 *            overwrite
	 * */

	public static void createTable(boolean overwrite) {
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(configuration);
			if (admin.tableExists(tableName)) {
				if (overwrite) {
					admin.disableTable(tableName);
					admin.deleteTable(tableName);
					System.out.println(tableName + "is exist ,delete ......");
					HTableDescriptor tableDescriptor = new HTableDescriptor(
							TableName.valueOf(tableName));
					tableDescriptor
							.addFamily(new HColumnDescriptor(familyname));
					admin.createTable(tableDescriptor);
				}
			} else {
				HTableDescriptor tableDescriptor = new HTableDescriptor(
						TableName.valueOf(tableName));
				tableDescriptor.addFamily(new HColumnDescriptor(familyname));
				admin.createTable(tableDescriptor);
			}
			System.out.println("end create table");
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Create table with families
	 * 
	 * @param overwrite
	 * @param familynames
	 */

	public static void createTable(boolean overwrite,
			Vector<String> familynames, String tableName) {
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(configuration);
			if (admin.tableExists(tableName)) {
				if (overwrite) {
					admin.disableTable(tableName);
					admin.deleteTable(tableName);
					System.out.println(tableName + "is exist ,delete ......");
					HTableDescriptor tableDescriptor = new HTableDescriptor(
							TableName.valueOf(tableName));

					for (String familyname : familynames) {
						tableDescriptor.addFamily(new HColumnDescriptor(
								familyname));
					}

					admin.createTable(tableDescriptor);
				}
			} else {
				HTableDescriptor tableDescriptor = new HTableDescriptor(
						TableName.valueOf(tableName));
				for (String familyname : familynames) {
					tableDescriptor
							.addFamily(new HColumnDescriptor(familyname));
				}
				admin.createTable(tableDescriptor);
			}
			System.out.println("end create table");
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Delete the existing table
	 * 
	 * @return
	 * */
	public static void dropTable() {
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(configuration);
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println(tableName + "delete success!");
			} else {
				System.out.println(tableName + "Table does not exist!");
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * insert a data
	 * 
	 * @param row
	 *            Row name
	 * @param valueName
	 *            Column name
	 * @param value
	 *            Content
	 * @return
	 * */
	public static void addData(String row, String valueName, String value) {
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(configuration);
			if (admin.tableExists(tableName)) {
				HTable table = new HTable(configuration, tableName);
				Put put = new Put(Bytes.toBytes(row));
				put.add(Bytes.toBytes(familyname), Bytes.toBytes(valueName),
						Bytes.toBytes(value));
				table.put(put);
				// System.out.println("add success!");
			} else {
				System.out.println(tableName + "Table does not exist!");
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Add data
	 * 
	 * @param row
	 * @param valueName
	 * @param value
	 * @param MyFamilyName
	 * @param MyTableName
	 */
	public static void addData(String row, String valueName, String value,
			String MyFamilyName, String MyTableName) {
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(configuration);
			if (admin.tableExists(MyTableName)) {
				HTable table = new HTable(configuration, MyTableName);
				Put put = new Put(Bytes.toBytes(row));
				put.add(Bytes.toBytes(MyFamilyName), Bytes.toBytes(valueName),
						Bytes.toBytes(value));
				table.put(put);
				// System.out.println("add success!");
			} else {
				System.out.println(tableName + "Table does not exist!");
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Add map in Hbase
	 * 
	 * @param row
	 * @param paras
	 */

	public static void addData(String row, Map<String, String> paras) {
		HBaseAdmin admin;

		try {
			admin = new HBaseAdmin(configuration);
			if (admin.tableExists(tableName)) {
				HTable table = new HTable(configuration, tableName);

				Iterator<Entry<String, String>> iter = paras.entrySet()
						.iterator();

				Put put = new Put(Bytes.toBytes(row));

				while (iter.hasNext()) {
					Map.Entry entry = (Map.Entry) iter.next();
					put.add(Bytes.toBytes(familyname),
							Bytes.toBytes(entry.getKey().toString()),
							Bytes.toBytes(entry.getValue().toString()));
				}
				table.put(put);
				// System.out.println("add success!");
			} else {
				System.out.println(tableName + "Table does not exist!");
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Delete a data
	 * 
	 * @param row
	 *            row name
	 * @return
	 * 
	 * */
	public static void deleteDate(String row) {
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(configuration);
			if (admin.tableExists(tableName)) {
				HTable table = new HTable(configuration, tableName);
				Delete delete = new Delete(Bytes.toBytes(row));
				table.delete(delete);
				System.out.println("delete success!");
			} else {
				System.out.println("Table does not exist!");
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * get a data
	 * 
	 * @param String
	 *            row
	 * */
	public static String getData(String row) {
		HTable table;
		String resultStr = "";
		try {
			table = new HTable(configuration, tableName);
			Get get = new Get(Bytes.toBytes(row));
			Result result = table.get(get);

			for (Cell cell : result.rawCells()) {

				// resultStr += new String(CellUtil.cloneQualifier(cell)) + "\n"
				// + new String(CellUtil.cloneValue(cell)) + "\n";
				resultStr = new String(CellUtil.cloneValue(cell));

			}
			// resultStr += new
			// String(CellUtil.cloneValue(result.rawCells()[0]));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return resultStr;
	}

	/**
	 * get a data
	 * 
	 * @param String
	 *            row
	 * */
	public static String getDataWithColumn(String row, String column) {
		HTable table;
		String resultStr = "";
		try {
			table = new HTable(configuration, tableName);
			Get get = new Get(Bytes.toBytes(row));
			Result result = table.get(get);
			for (Cell cell : result.rawCells()) {
				if (new String(CellUtil.cloneQualifier(cell)).equals(column)) {
					resultStr += new String(CellUtil.cloneQualifier(cell))
							+ "\n" + new String(CellUtil.cloneValue(cell))
							+ "\n";
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return resultStr;
	}

	/**
	 * get a data
	 * 
	 * @param String
	 *            row1
	 * @param String
	 *            row2
	 * */
	public static int getData(String row1, String row2) {
		HTable table;
		int times = 0;
		try {
			table = new HTable(configuration, tableName);
			Get get = new Get(Bytes.toBytes(row1));
			Result result = table.get(get);

			for (Cell cell : result.rawCells()) {
				if (new String(CellUtil.cloneQualifier(cell)).contains(row2)) {
					times++;
				}

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return times;
	}

	/**
	 * display all data
	 *
	 * @return
	 * */
	public static void displayAllData() {
		HTable table;
		try {
			table = new HTable(configuration, tableName);
			Scan scan = new Scan();
			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				for (Cell cell : result.rawCells()) {
					System.out.println("RowName:"
							+ new String(CellUtil.cloneRow(cell)) + " ");
					System.out.println("Timetamp:" + cell.getTimestamp() + " ");
					System.out.println("column Family:"
							+ new String(CellUtil.cloneFamily(cell)) + " ");
					System.out.println("row Name:"
							+ new String(CellUtil.cloneQualifier(cell)) + " ");
					System.out.println("value:"
							+ new String(CellUtil.cloneValue(cell)) + " ");
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * get ResultScanner result
	 *
	 * @return a ResultScanner
	 * */
	public static ResultScanner getAllData() {
		HTable table;
		try {
			table = new HTable(configuration, tableName);
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes("AnnualGroup"));
			ResultScanner results = table.getScanner(scan);
			return results;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * get ResultScanner result
	 *
	 * @param scan
	 *
	 * @return a ResultScanner
	 * */
	public static ResultScanner getAllData(Scan scan) {
		HTable table;
		try {
			table = new HTable(configuration, tableName);
			ResultScanner results = table.getScanner(scan);
			return results;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Get data size
	 *
	 * @return int size
	 * */
	public static int getDataLength() {
		HTable table;
		int size = 0;
		try {
			table = new HTable(configuration, tableName);
			Scan scan = new Scan();
			ResultScanner results = table.getScanner(scan);

			for (Result result : results) {
				for (Cell cell : result.rawCells()) {
					size++;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return size;
	}

}
