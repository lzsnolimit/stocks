package Stock;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.json.JSONObject;

public class AnylyseGroup {

	private static String TableName = "StocksGroup";
	private static String MysqlDataBase = "StocksGroup";
	private static String MysqlTablename;

	/**
	 * Map class
	 * 
	 * @author Zhongshan Lu
	 *
	 */
	public static class MyMapper extends TableMapper<FloatWritable, Text> {

		/**
		 * map method
		 */
		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {

			// System.out.println(value.size());

			Map<String, String> jObject = new HashMap<String, String>();
			Float difference = (float) 0;

			try {
				// System.out.println(new
				// String(CellUtil.cloneValue(value.rawCells()[1])));
				if (value.size() == 2) {
					jObject.put(
							"OriginalTime",
							new String(
									CellUtil.cloneQualifier(value.rawCells()[0])));
					jObject.put(
							"NewTime",
							new String(
									CellUtil.cloneQualifier(value.rawCells()[1])));
					jObject.put("StockName",
							new String(CellUtil.cloneRow(value.rawCells()[0])));
					jObject.put(
							"OriginalRate",
							new String(CellUtil.cloneValue(value.rawCells()[0])));
					jObject.put(
							"NewRate",
							new String(CellUtil.cloneValue(value.rawCells()[1])));
					difference = Float.valueOf(new String(CellUtil
							.cloneValue(value.rawCells()[1])))
							- Float.valueOf(new String(CellUtil
									.cloneValue(value.rawCells()[0])));

					jObject.put(
							"DifferenceRate",
							String.valueOf(difference
									/ Float.valueOf(new String(CellUtil
											.cloneValue(value.rawCells()[0])))));
					jObject.put("Difference", String.valueOf(difference / 1000));
				} else {
					jObject.put(
							"NewTime",
							new String(
									CellUtil.cloneQualifier(value.rawCells()[0])));
					jObject.put("StockName",
							new String(CellUtil.cloneRow(value.rawCells()[0])));
					jObject.put(
							"NewRate",
							new String(CellUtil.cloneValue(value.rawCells()[0])));
					difference = Float.valueOf(new String(CellUtil
							.cloneValue(value.rawCells()[0])));
					jObject.put("Difference", String.valueOf(difference / 1000));
				}
				// System.out.println(difference.toString()+jObject.toString());
				// context.write(new FloatWritable(difference),
				// new Text(jObject.toString()));
				MySql.insertData(MysqlDataBase, MysqlTablename, jObject);
			} catch (Exception e) {
				// System.out.println();
				e.printStackTrace();
				// TODO: handle exception
			}
			// context.write(new FloatWritable(Long.parseLong(content)), new
			// Text());
		}

	}

	/**
	 * Reducer class
	 * 
	 * @author Zhongshan lu
	 *
	 */
	public static class MyTableReducer extends
			TableReducer<FloatWritable, Text, ImmutableBytesWritable> {

		/**
		 * reduce method
		 */
		public void reduce(FloatWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			// System.out.println(key);
			for (Text json : values) {
				System.out.println(key.toString() + "   " + json.toString());
			}
		}
	}

	/**
	 * Start analysing process
	 * 
	 * @param FamilyName
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void start(String type, String FamilyName)
			throws IOException, ClassNotFoundException, InterruptedException {
		MysqlTablename = FamilyName
				+ new java.text.SimpleDateFormat("yyyyMMddHHmmss")
						.format(new Date(System.currentTimeMillis()));
		Set<String> keys = new HashSet<String>();
		keys.add("OriginalTime");
		keys.add("NewTime");
		keys.add("OriginalRate");
		keys.add("NewRate");
		keys.add("StockName");
		keys.add("DifferenceRate");
		keys.add("Difference");
		MySql.createTable(MysqlDataBase, MysqlTablename, keys);
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "StockSummary");
		job.setJarByClass(App.class); // class that contains mapper and reducer
		Scan scan = new Scan();
		Filter FamilyFilter = new FamilyFilter(CompareOp.EQUAL,
				new SubstringComparator(FamilyName));
		Map<String, String> TimeConfig = readConfig(type);
		Filter ColumnFilter = new QualifierFilter(
				CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator(Bytes.toBytes(TimeConfig.get("secondNew"))));
		FilterList fList = new FilterList();
		fList.addFilter(FamilyFilter);
		fList.addFilter(ColumnFilter);

		scan.setFilter(fList);
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob(TableName, // input table
				scan, // Scan instance to control CF and attribute selection
				MyMapper.class, // mapper class
				FloatWritable.class, // mapper output key
				Text.class, // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob(TableName, // output
				// table
				MyTableReducer.class, // reducer class
				job);
		job.setNumReduceTasks(1); // at least one, adjust as required

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

	/**
	 * Read config file
	 * 
	 * @return
	 * @throws IOException
	 */
	public static Map<String, String> readConfig(String type)
			throws IOException {
		Map<String, String> config = new HashMap<String, String>();
		File GroupFile = new File("groups/" + type + "config");
		if (GroupFile.exists()) {
			BufferedReader in = new BufferedReader(new FileReader(GroupFile));
			String line = in.readLine();
			while (line != null) {
				String[] pairs = line.split("\\=");
				// System.out.println(pairs[0]+pairs[1]);
				config.put(pairs[0], pairs[1]);
				line = in.readLine();
			}
			in.close();
		} else {
			System.out.println("No config file");
		}
		return config;
	}
	
	
	


}