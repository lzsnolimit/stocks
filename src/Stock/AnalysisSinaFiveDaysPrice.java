package Stock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.json.JSONObject;

public class AnalysisSinaFiveDaysPrice {

	private final static String HbaseTableName = "StocksFiveDaysPrice";
	private final static String MysqlDatabase = "FiveDaysPrice";
	private final static Double HighPoint = 0.04;
	private final static Double LowPoint = -0.04;
	private final static int period = 5; // 5 minutes
	private final static int lastPeriodLength = 1;
	private static String AnalyseType;

	
	
	private static List<List<Long>> HighestPeriodTimes = new ArrayList<List<Long>>();
	private static List<List<Long>> LowestPeriodTimes = new ArrayList<List<Long>>();

	
	
	// public static String currentTime = new java.text.SimpleDateFormat(
	// "yyyy-MM-dd").format(new Date(System.currentTimeMillis()));
	public static String currentTime = "2015-08-28";
	private static String MysqlTable;
	public static Boolean TableCreated = false;

	public static class MyMapper extends TableMapper<Text, DoubleWritable> {
		/**
		 * map method
		 */
		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {

			// parseFiveDays(new
			// String(CellUtil.cloneValue(value.rawCells()[0])));

			for (Cell cell : value.rawCells()) {

				// System.out.println();
				try {

					parseFiveDays(new String(CellUtil.cloneRow(cell)),
							new String(CellUtil.cloneValue(cell)));

				} catch (Exception e) {
					System.out.println(new String(CellUtil.cloneRow(cell))
							+ " error");
				}

			}
			// System.out.println(value.rawCells().length);
		}
	}

	/**
	 * new String(CellUtil.cloneRow(cell)), new String(
	 * CellUtil.cloneValue(cell)) Reducer class
	 * 
	 * @author Zhongshan lu
	 *
	 */
	public static class MyTableReducer extends
			TableReducer<Text, DoubleWritable, ImmutableBytesWritable> {
		/**
		 * reduce method
		 */
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

		}
	}

	/**
	 * Start map reduce job
	 * 
	 * @param type
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void start(String type) throws IOException,
			ClassNotFoundException, InterruptedException {
		AnalyseType = type;
		MysqlTable = AnalyseType
				+ new java.text.SimpleDateFormat("yyyyMMdd").format(new Date(
						System.currentTimeMillis()));
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "StockHisSummary");
		job.setJarByClass(App.class); // class that contains mapper and reducer
		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		// System.out.println(type);
		Filter FamilyFilter = new FamilyFilter(CompareOp.EQUAL,
				new SubstringComparator(type));
		Filter ColumnFilter = new QualifierFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(currentTime)));
		FilterList fList = new FilterList();
		fList.addFilter(FamilyFilter);
		fList.addFilter(ColumnFilter);
		scan.setFilter(fList);
		TableMapReduceUtil.initTableMapperJob(HbaseTableName, // input table
				scan, // Scan instance to control CF and attribute selection
				MyMapper.class, // mapper class
				Text.class, // mapper output key
				FloatWritable.class, // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob(HbaseTableName, // output
				MyTableReducer.class, // reducer class
				job);
		job.setNumReduceTasks(1); // at least one, adjust as required

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

	public static Map<Long, Map<String, Double>> parseFiveDays(String symbol,
			String value) throws Exception {
		// System.out.println(symbol);
		JSONObject DaysObject = new JSONObject(value);
		List<String> keys = new ArrayList(DaysObject.keySet());
		Collections.sort(keys);
		Map<String, String> FiveDays = new HashMap<String, String>();
		Double highPointRate = 0.0;
		Double lowPointRate = 0.0;
		Double highPointEverage = 0.0;
		Double lowPointEverage = 0.0;
		Double oneRate = 1.0 / new Double(keys.size());

		for (String key : keys) {
			// System.out.println(key);
			Map<String, String> oneDaySummary = parseOneDay(DaysObject
					.getJSONObject(key).toString());
			if (oneDaySummary.size() == 0) {
				continue;
			}
			try {
				highPointEverage += Double.parseDouble(oneDaySummary
						.get("HighestPoint")) / keys.size();
				lowPointEverage += Double.parseDouble(oneDaySummary
						.get("LowestPoint")) / keys.size();
			} catch (Exception e) {
				System.out.println(symbol + " error!");
			}

			if (Double.parseDouble(oneDaySummary.get("HighestPoint")) > HighPoint) {
				// System.out.println(oneDaySummary.get("HighestPoint"));
				// System.out.println(oneRate);
				highPointRate += oneRate;
			} else if (Double.parseDouble(oneDaySummary.get("LowestPoint")) < LowPoint) {
				// System.out.println(oneDaySummary.get("LowestPoint"));
				// System.out.println(oneRate);
				lowPointRate += oneRate;
			}
			for (Entry<String, String> pair : oneDaySummary.entrySet()) {
				if (FiveDays.containsKey(pair.getKey())) {
					FiveDays.put(pair.getKey(), FiveDays.get(pair.getKey())
							+ " " + oneDaySummary.get(pair.getKey()));
				} else {
					FiveDays.put(pair.getKey(),
							oneDaySummary.get(pair.getKey()));
				}
			}
		}
		FiveDays.put("highPointRate", String.valueOf(highPointRate));
		FiveDays.put("lowPointRate", String.valueOf(lowPointRate));
		FiveDays.put("highPointEverage", String.valueOf(highPointEverage));
		FiveDays.put("lowPointEverage", String.valueOf(lowPointEverage));
		FiveDays.put("symbol", symbol);

		for (Entry<String, String> pair : FiveDays.entrySet()) {
			// System.out.println(pair.getKey() + pair.getValue());
		}
		if (!TableCreated) {
			MySql.createTable(MysqlDatabase, MysqlTable, FiveDays.keySet());
			TableCreated = true;
		}

		MySql.insertData(MysqlDatabase, MysqlTable, FiveDays);
		return null;
	}

	public static Map<String, String> parseOneDay(String value) {
		// System.out.println(value);

		// System.out.println(periodPrices);
		Map<String, String> oneDaySummary = new HashMap<String, String>();
		parseOneDayPoints(value, oneDaySummary);
		return oneDaySummary;
	}

	public static void parseOneDayPoints(String value,
			Map<String, String> oneDaySummary) {
		JSONObject DayObject = new JSONObject(value);
		Double LastClose = Double.parseDouble(DayObject.getString("LastClose"));
		List<String> keys = new ArrayList(DayObject.keySet());
		if (keys.contains("LastClose")) {
			keys.remove("LastClose");
		}
		if (DayObject.length() <= period) {
			return;
		}
		Double totalVolumn = 0.0;
		Double totalFunding = 0.0;
		Double UpHighPointVolumn = 0.0;
		Double DownLowPointVolumn = 0.0;
		Double HighestPoint = -1.0;
		Double LowestPoint = 1.0;
		Long HighestTime = (long) 0;
		Long LowestTime = (long) 0;
		Collections.sort(keys);
		Double periodAveragePrice = 0.0;
		Double periodTotalVolumn = 0.0;
		Double periodTotalFunding = 0.0;
		Double LowVolume = 0.0;
		Double UpVolume = 0.0;

		Long LastPeriodHighestTime = (long) 0;
		Long LastPeriodLowestTime = (long) 0;
		Double LastPeriodHighestRate = -1.0;
		Double LastPeriodLowestRate = 1.0;

		Double HighestPeriodVolume = 0.0;
		Double LowestPeriodVolume = 0.0;
		Double HighestPeriodRate = -1.0;
		Double LowestPeriodRate = 1.0;
		Long HighestPeriodTime = (long) 0;
		Long LowestPeriodTime = (long) 0;

		JSONObject periodPrices = new JSONObject();
		JSONObject onePeriod = new JSONObject();

		// First period
		for (int i = 0; i < period; i++) {
			String key = keys.get(i);
			Double currentVolumn = DayObject.getJSONObject(key).getDouble(
					"volume");
			Double currentPrice = DayObject.getJSONObject(key).getDouble(
					"price");
			periodAveragePrice += currentPrice / period;
			periodTotalVolumn += currentVolumn;
			periodTotalFunding += currentVolumn * currentPrice;
		}
		onePeriod.put("price", String.valueOf(periodAveragePrice));
		onePeriod.put("volumn", String.valueOf(periodTotalVolumn));
		onePeriod.put("funding", String.valueOf(periodTotalFunding));
		periodPrices.put(keys.get(0), onePeriod);
		// System.out.println(periodPrices.toString());

		// One minute analyse
		for (int i = 0; i < keys.size(); i++) {
			String key = keys.get(i);
			Double currentVolumn = DayObject.getJSONObject(key).getDouble(
					"volume");
			Double currentPrice = DayObject.getJSONObject(key).getDouble(
					"price");
			totalVolumn += currentVolumn;
			totalFunding += currentVolumn * currentPrice;
			Double rate = (currentPrice - LastClose) / LastClose;
			if (HighestPoint < rate) {
				HighestPoint = rate;
				HighestTime = Long.parseLong(key);
			} else if (LowestPoint > rate) {
				LowestPoint = rate;
				LowestTime = Long.parseLong(key);
			}
			if (rate > HighPoint) {
				UpHighPointVolumn += currentVolumn;
			} else if (rate < LowPoint) {
				DownLowPointVolumn += currentVolumn;
			}
			if (rate > 0) {
				UpVolume += currentVolumn;
			} else {
				LowVolume += currentVolumn;
			}

			// Create period list
			if (i > 0 && i < keys.size() - (period - 1)) {
				periodAveragePrice = periodAveragePrice
						+ DayObject.getJSONObject(keys.get(i + 4)).getDouble(
								"price")
						/ period
						- DayObject.getJSONObject(keys.get(i - 1)).getDouble(
								"price") / period;

				periodTotalVolumn = periodTotalVolumn
						+ DayObject.getJSONObject(keys.get(i + 4)).getDouble(
								"volume")
						- DayObject.getJSONObject(keys.get(i - 1)).getDouble(
								"volume");

				periodTotalFunding = periodTotalFunding
						+ DayObject.getJSONObject(keys.get(i + 4)).getDouble(
								"price")
						* DayObject.getJSONObject(keys.get(i + 4)).getDouble(
								"volume")
						- DayObject.getJSONObject(keys.get(i - 1)).getDouble(
								"price")
						* DayObject.getJSONObject(keys.get(i - 1)).getDouble(
								"volume");
				onePeriod = new JSONObject();
				onePeriod.put("price", String.valueOf(periodAveragePrice));
				onePeriod.put("volumn", String.valueOf(periodTotalVolumn));
				onePeriod.put("funding", String.valueOf(periodTotalFunding));
				periodPrices.put(key, onePeriod);
			}
		}

		/**
		 * Periods Analyse
		 */

		for (String key : periodPrices.keySet()) {
			if (key.equals("period")) {
				continue;
			}
			int lengthToClose = 0;
			try {
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(new Date(Long.parseLong(key)));
				int hour = calendar.get(Calendar.HOUR_OF_DAY);
				// System.out.println();
				switch (AnalyseType) {
				case "US":
					lengthToClose = 16 - hour;
					break;
				case "CN":
					lengthToClose = 15 - hour;
					break;
				default:
					break;
				}
			} catch (Exception e) {
				continue;
			}

			if (lengthToClose < 0) {
				System.out.println("Error:");
				System.out.println(value);
				System.out.println(key);
				continue;
			}

			Double funding = Double.parseDouble(periodPrices.getJSONObject(key)
					.getString("funding"));
			Double PriceRate = (Double.parseDouble(periodPrices.getJSONObject(
					key).getString("price")) - LastClose)
					/ LastClose;
			Double volumn = Double.parseDouble(periodPrices.getJSONObject(key)
					.getString("volumn"));

			if (PriceRate > HighestPeriodRate) {
				HighestPeriodRate = PriceRate;
				HighestPeriodTime = Long.parseLong(key);
				HighestPeriodVolume = volumn;
			} else if (PriceRate < LowestPeriodRate) {
				LowestPeriodRate = PriceRate;
				LowestPeriodTime = Long.parseLong(key);
				LowestPeriodVolume = volumn;
			}
			// System.out.println(lengthToClose);
			if (lengthToClose <= lastPeriodLength) {
				if (LastPeriodHighestRate < PriceRate) {
					LastPeriodHighestRate = PriceRate;
					LastPeriodHighestTime = Long.parseLong(key);
				} else if (LastPeriodLowestRate > PriceRate) {
					LastPeriodLowestRate = PriceRate;
					LastPeriodLowestTime = Long.parseLong(key);
				}
			}
		}
		oneDaySummary.put("totalVolumn", String.valueOf(totalVolumn));
		oneDaySummary.put("totalFunding", String.valueOf(totalFunding));
		oneDaySummary.put("UpHighPointVolumn",
				String.valueOf(UpHighPointVolumn));
		oneDaySummary.put("DownLowPointVolumn",
				String.valueOf(DownLowPointVolumn));
		oneDaySummary.put("HighestPoint", String.valueOf(HighestPoint));
		oneDaySummary.put("LowestPoint", String.valueOf(LowestPoint));

		oneDaySummary.put("HighestTime", new java.text.SimpleDateFormat(
				"HH:mm:ss").format(new Date(HighestTime)));
		oneDaySummary.put("LowestTime", new java.text.SimpleDateFormat(
				"HH:mm:ss").format(new Date(LowestTime)));

		oneDaySummary.put("EveragePrice",
				String.valueOf(totalFunding / totalVolumn));
		oneDaySummary.put("DownLowPointVolumn",
				String.valueOf(DownLowPointVolumn));
		oneDaySummary.put("UpVolume", String.valueOf(UpVolume));

		oneDaySummary.put("LastPeriodHighestTime",
				new java.text.SimpleDateFormat("HH:mm:ss").format(new Date(
						LastPeriodHighestTime)));
		oneDaySummary.put("LastPeriodLowestTime",
				new java.text.SimpleDateFormat("HH:mm:ss").format(new Date(
						LastPeriodLowestTime)));

		oneDaySummary.put("LastPeriodHighestRate",
				String.valueOf(LastPeriodHighestRate));
		oneDaySummary.put("LastPeriodLowestRate",
				String.valueOf(LastPeriodLowestRate));

		oneDaySummary.put("HighestPeriodVolume",
				String.valueOf(HighestPeriodVolume));
		oneDaySummary.put("LowestPeriodVolume",
				String.valueOf(LowestPeriodVolume));
		oneDaySummary.put("HighestPeriodRate",
				String.valueOf(HighestPeriodRate));
		oneDaySummary.put("LowestPeriodRate", String.valueOf(LowestPeriodRate));

		oneDaySummary.put("HighestPeriodTime", new java.text.SimpleDateFormat(
				"HH:mm:ss").format(new Date(HighestPeriodTime)));
		oneDaySummary.put("LowestPeriodTime", new java.text.SimpleDateFormat(
				"HH:mm:ss").format(new Date(LowestPeriodTime)));


		// 初始化 Calendar 对象，但并不必要，除非需要重置时间
		// calendar.setTime(new Date(LastPeriodLowestTime));
		// int HOUR = calendar.get(Calendar.HOUR_OF_DAY);
		// // System.out.println(MONTH);
		// // 1440191160000
		// if (HOUR == 17) {
		// calendar.setTime(new Date(LastPeriodLowestTime));
		// HOUR = calendar.get(Calendar.HOUR_OF_DAY);
		// System.out.println(HOUR);
		//
		// System.out.println(LastPeriodLowestTime);
		// // System.out.println(LastPeriodHighestTime);
		// // System.out.println(value);
		// }
		// System.out.println(value);
		// for (Entry<String, String> pair : oneDaySummary.entrySet()) {
		// System.out.println(pair.getKey() + pair.getValue());
		// }
		// System.out.println(periodPrices.toString());
	}

	public static void analysePeriodsTimes() {
		System.out.println("High period:");
		
	}
}
