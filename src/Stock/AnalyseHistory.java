package Stock;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.jruby.RubyBoolean.True;
import org.json.JSONArray;
import org.json.JSONException;
import org.netlib.util.intW;

public class AnalyseHistory {
	private final static String HbaseTableName = "StocksHisShort";
	private static int PeriodRang = 5;// One year is around 250 trading
											// days
	private final static Double DropsAndRaisesRate = 0.055;
	private final static String MysqlDatabase = "History";
	private final static Double waitingPeriod = 3.0;
	private static boolean OneDayOneStockTableCreated = false;
	private static boolean OneStockTwoDayTableCreated = false;
	private static boolean OneStockDropRiseTableCreated = false;
	private static String OneDayOneStockMysqlTableName;
	private static String OneStockDropRiseTableName;
	private static String OneStockTwoDayTableName;
	private static String DropsAndRaisesRangeTableName;
	private static Vector<Map<String, Double>> oneStockDropRiseRates = new Vector<Map<String, Double>>();
	private static Map<String, Double> KongList = new TreeMap<String, Double>();
	private final static Double leastEstCap = 200000.0;
	private static List<List<Double>> NumberRates = new ArrayList<List<Double>>();
	private static Map<Integer, Integer> DropsAndRaisesFrequency = new TreeMap<Integer, Integer>();
	/**
	 * Map class
	 * 
	 * @author Zhongshan Lu
	 *
	 */

	static {
		readMaikong();
	}

	public static class MyMapper extends TableMapper<Text, DoubleWritable> {
		/**
		 * map method
		 */
		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {

			// Map<String, Double> rates = oneStockDropRiseAnalyse(new String(
			// CellUtil.cloneRow(value.rawCells()[0])), new String(
			// CellUtil.cloneValue(value.rawCells()[0])));
			oneStockTwoDayCloseHighAnalyse(
					new String(CellUtil.cloneRow(value.rawCells()[0])),
					new String(CellUtil.cloneValue(value.rawCells()[0])));
			// OneDayOneStockanalyse(
			// new String(CellUtil.cloneRow(value.rawCells()[0])),
			// new String(CellUtil.cloneValue(value.rawCells()[0])));

			// oneStockDropsAndRaisesAnalyse(
			// new String(CellUtil.cloneRow(value.rawCells()[0])),
			// new String(CellUtil.cloneValue(value.rawCells()[0])));
		}
	}

	/**
	 * Reducer class
	 * 
	 * @author Zhongshan lu ；
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
	 * Start analysing process
	 * 
	 * @param FamilyName
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void start(String type,Integer range) throws IOException,
			ClassNotFoundException, InterruptedException {
		PeriodRang=range;
		OneStockDropRiseTableName = type
				+ new java.text.SimpleDateFormat("yyyyMMddHHmmss")
						.format(new Date(System.currentTimeMillis()))
				+ "OneStockDropRise" + PeriodRang + "days";
		OneDayOneStockMysqlTableName = type
				+ new java.text.SimpleDateFormat("yyyyMMddHHmmss")
						.format(new Date(System.currentTimeMillis()))
				+ "OneDayRatesReport" + PeriodRang + "days";
		OneStockTwoDayTableName = type
				+ new java.text.SimpleDateFormat("yyyyMMddHHmmss")
						.format(new Date(System.currentTimeMillis()))
				+ "OneStockTwoDay" + PeriodRang + "days";

		DropsAndRaisesRangeTableName = type
				+ new java.text.SimpleDateFormat("yyyyMMddHHmmss")
						.format(new Date(System.currentTimeMillis()))
				+ "DropsAndRaisesRange" + PeriodRang + "days";
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "StockHisSummary");
		job.setJarByClass(App.class); // class that contains mapper and reducer
		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		Filter ColumnFilter = new QualifierFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(type)));
		// System.out.println(type);
		scan.setFilter(ColumnFilter);
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

		OneDayOneStockTableCreated = false;
		OneStockTwoDayTableCreated = false;
		OneStockDropRiseTableCreated = false;
		if (!b) {
			throw new IOException("error with job!");
		}

	}

	/**
	 * Analyse one day stock
	 * 
	 * @param symbol
	 * @param arrayHis
	 */
	public static void OneDayOneStockanalyse(String symbol, String arrayHis) {

		JSONArray historyInfos = new JSONArray(arrayHis);
		List<Double> lowRates = new ArrayList<Double>();
		List<Double> highRates = new ArrayList<Double>();
		List<Double> closeRates = new ArrayList<Double>();

		Double open = 0.0, high = 0.0, low = 0.0, close = 0.0;
		int lengthRang = 0;
		if (PeriodRang > historyInfos.length()) {
			lengthRang = historyInfos.length();
		} else {
			lengthRang = PeriodRang;
		}

		for (int i = 0; i <= lengthRang; i++) {

			try {
				open = parseDouble(historyInfos.getJSONArray(i).getString(1));
				high = parseDouble(historyInfos.getJSONArray(i).getString(2));
				low = parseDouble(historyInfos.getJSONArray(i).getString(3));
				close = parseDouble(historyInfos.getJSONArray(i).getString(4));
				highRates.add(high / open - 1);
				lowRates.add(low / open - 1);
				closeRates.add(close / open - 1);
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
		// System.out.println(symbol);
		//
		// Collections.sort(lowRates);
		// Collections.sort(highRates);
		// Collections.sort(closeRates);

		Double lastPrice = 0.0;
		try {
			lastPrice = parseDouble(historyInfos.getJSONArray(0).getString(4));
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			// System.out.println(symbol);
			// e.printStackTrace();
		} catch (Exception e) {
			// System.out.println(symbol);
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}

		try {
			if (System.currentTimeMillis()
					- new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(
							historyInfos.getJSONArray(0).getString(0))
							.getTime() > 433782878) {
				return;
			}

		} catch (Exception e) {

		}

		try {
			if (Integer.parseInt(historyInfos.getJSONArray(0).getString(5))
					* Integer.parseInt(historyInfos.getJSONArray(0)
							.getString(4)) < leastEstCap) {
				return;
			}
			System.out.println(Integer.parseInt(historyInfos.getJSONArray(0)
					.getString(5))
					* Integer.parseInt(historyInfos.getJSONArray(0)
							.getString(4)));
		} catch (Exception e) {
			// TODO: handle exception
		}
		RatesReport(symbol, "low", lowRates, lastPrice,
				OneDayOneStockMysqlTableName);
		RatesReport(symbol, "high", highRates, lastPrice,
				OneDayOneStockMysqlTableName);
		RatesReport(symbol, "close", closeRates, lastPrice,
				OneDayOneStockMysqlTableName);

	}

	/**
	 * Parse double number
	 * 
	 * @param num
	 * @return
	 * @throws Exception
	 */

	public static double parseDouble(String num) throws Exception {
		num = num.replace(" ", "");
		return Double.parseDouble(num);

	}

	/**
	 * Report one day analyse
	 * 
	 * @param symbol
	 * @param type
	 * @param rates
	 * @param lastPrice
	 * @return
	 */
	public static boolean RatesReport(String symbol, String type,
			List<Double> rates, Double lastPrice, String tableName) {
		if (rates.size() == 0) {
			return false;
		}
		Double Total = 0.0, Average = 0.0, HighestRate = 0.0, ZERO = 0.0, ONE = 0.0, TWO = 0.0, THREE = 0.0, FOUR = 0.0, OTHER = 0.0, Size = (double) rates
				.size();

		for (Double rate : rates) {
			Total += rate;

			int roundNum = new Double(rate * 100).intValue();
			if (Math.abs(rate) > Math.abs(HighestRate)) {
				HighestRate = rate;
			}
			switch (roundNum) {
			case 0:
				ZERO++;
				break;
			case 1:
				ONE++;
				break;
			case 2:
				TWO++;
				break;
			case 3:
				THREE++;
				break;
			case 4:
				FOUR++;
				break;
			default:
				OTHER++;
				break;
			}
		}
		Average = Total / Size;
		Double standardDevition = getStandardDevition(rates, Average);

		Map<String, Double> dailyRates = new TreeMap<String, Double>();
		dailyRates.put("average", Average);
		dailyRates.put("highestRate", HighestRate);
		dailyRates.put("ZERO", ZERO / Size);
		dailyRates.put("ONE", ONE / Size);
		dailyRates.put("TWO", TWO / Size);
		dailyRates.put("THREE", THREE / Size);
		dailyRates.put("FOUR", FOUR / Size);
		dailyRates.put("OTHER", OTHER / Size);
		dailyRates.put("standardDevition", standardDevition / Size);
		dailyRates.put("Size", Size);
		dailyRates.put("LastPrice", lastPrice);
		if (KongList.containsKey(symbol)) {
			dailyRates.put("Kong", KongList.get(symbol));
		} else {
			dailyRates.put("Kong", 1.0);
		}
		// System.out.println(type);
		// for (Entry<String, Double> pair : dailyRates.entrySet()) {
		// System.out.println(pair.getKey() + "  " + pair.getValue());
		// }

		Map<String, String> StrPair = new HashMap<String, String>();
		StrPair.put("Symbol", symbol);

		StrPair.put("Type", type);
		if (!OneDayOneStockTableCreated) {
			MySql.createNumberTable(MysqlDatabase, tableName,
					dailyRates.keySet(), StrPair.keySet());
			OneDayOneStockTableCreated = true;
		}
		MySql.insertNumberData(MysqlDatabase, tableName, dailyRates, StrPair);
		return true;
	}

	/**
	 * Get standard devition
	 * 
	 * @param rates
	 * @param average
	 * @return
	 */
	public static double getStandardDevition(List<Double> rates, Double average) {
		double sum = 0;
		for (Double rate : rates) {
			sum += Math.pow(rate - average, 2);
		}
		return Math.sqrt(sum);
	}

	/**
	 * One stock drop or raise analyse
	 * 
	 * @param symbol
	 * @param arrayHis
	 */
	public static Map<String, Double> oneStockDropRiseAnalyse(String symbol,
			String arrayHis) {
		JSONArray historyInfos = new JSONArray(arrayHis);

		int lengthRang = 0;
		if (PeriodRang > historyInfos.length()) {
			lengthRang = historyInfos.length();
		} else {
			lengthRang = PeriodRang;
		}
		for (int i = lengthRang - 1; i > waitingPeriod + 2; i--) {

			try {
				Double changeRate = getChangeRate(historyInfos.getJSONArray(i),
						historyInfos.getJSONArray(i - 1));
				if (Math.abs(changeRate) > DropsAndRaisesRate) {
					Double low = 10000000.0, high = 0.0, highDate = 0.0, lowDate = 0.0;
					for (int j = i - 2; j > i - 2 - waitingPeriod; j--) {
						Double temHigh = parseDouble(historyInfos.getJSONArray(
								j).getString(2));
						Double temLow = parseDouble(historyInfos
								.getJSONArray(j).getString(3));
						if (high < temHigh) {
							high = temHigh;
							highDate = (double) ((i - 1) - j);
						}

						if (low > temLow) {
							low = temLow;
							lowDate = (double) ((i - 1) - j);
						}
					}

					Map<String, Double> rates = new TreeMap<String, Double>();
					rates.put("changeRate", changeRate);
					rates.put("ranglowest", low
							/ parseDouble(historyInfos.getJSONArray(i - 1)
									.getString(4)) - 1);
					rates.put("ranghighest", high
							/ parseDouble(historyInfos.getJSONArray(i - 1)
									.getString(4)) - 1);
					rates.put("lowestDate", lowDate);
					rates.put("highestDate", highDate);
					rates.put("period", waitingPeriod);
					if (KongList.containsKey(symbol)) {
						rates.put("Kong", KongList.get(symbol));
					} else {
						rates.put("Kong", 1.0);
					}
					for (Entry<String, Double> pair : rates.entrySet()) {
						// System.out.println(pair.getKey() + " "
						// + pair.getValue());
					}

					if (!OneStockDropRiseTableCreated) {
						MySql.createNumberTable(MysqlDatabase,
								OneStockDropRiseTableName, rates.keySet(),
								"Symbol");
						OneStockDropRiseTableCreated = true;
					}

					Map<String, String> StrPair = new HashMap<String, String>();
					StrPair.put("Symbol", symbol);
					MySql.insertNumberData(MysqlDatabase,
							OneStockDropRiseTableName, rates, StrPair);
					oneStockDropRiseRates.add(rates);
					return rates;
				}
			} catch (Exception e) {

				// TODO: handle exception
			}
		}
		return null;
	}

	/**
	 * Get the change rate
	 * 
	 * @param dayOne
	 * @param daytwo
	 * @return
	 * @throws JSONException
	 * @throws Exception
	 */
	public static Double getChangeRate(JSONArray dayOne, JSONArray daytwo)
			throws JSONException, Exception {
		Double rate = parseDouble(daytwo.getString(4))
				/ parseDouble(dayOne.getString(4)) - 1;
		return rate;
	}

	public static Map<String, Double> fiveDaysAnalyse() {

		return null;

	}

	/**
	 * Report one stock drop rise
	 */
	public static void oneStockDropRiseReport() {
		Double dropAverageLow = 0.0, dropAverageHigh = 0.0, raiseAverageLow = 0.0, raiseAverageHigh = 0.0, dropAverageLowDate = 0.0, dropAverageHighDate = 0.0, raiseAverageLowDate = 0.0, raiseAverageHighDate = 0.0, raiseSize = 0.0, dropSize = 0.0;
		for (Map<String, Double> rate : oneStockDropRiseRates) {
			if (rate.get("changeRate") > 0) {

				if (Math.abs(rate.get("ranglowest")) < 0.5) {
					raiseSize++;
					raiseAverageHigh += rate.get("ranghighest");
					raiseAverageHighDate += rate.get("highestDate");
					raiseAverageLow += rate.get("ranglowest");
					raiseAverageLowDate += rate.get("lowestDate");
				}

			} else {
				if (rate.get("ranghighest") < 0.5) {
					dropSize++;
					dropAverageHigh += rate.get("ranghighest");
					dropAverageHighDate += rate.get("highestDate");
					dropAverageLow += rate.get("ranglowest");
					dropAverageLowDate += rate.get("lowestDate");
				}

			}
		}

		System.out.println("Raise: high point:" + raiseAverageHigh / raiseSize
				+ " at " + raiseAverageHighDate / raiseSize + "low point:"
				+ raiseAverageLow / raiseSize + " at " + raiseAverageLowDate
				/ raiseSize);
		System.out.println("Drop: high point:" + dropAverageHigh / dropSize
				+ " at " + dropAverageHighDate / dropSize + "low point:"
				+ dropAverageLow / dropSize + " at " + dropAverageLowDate
				/ dropSize);

		raiseSize = 0.0;
		dropSize = 0.0;
		Double raiseLevel = 0.0, dropLevel = 0.0, level = 0.05;
		for (Map<String, Double> rate : oneStockDropRiseRates) {
			if (rate.get("changeRate") > 0) {
				raiseSize++;
				if (Math.abs(rate.get("ranghighest")) > level) {
					raiseLevel++;
				}

			} else {
				dropSize++;
				if (rate.get("ranghighest") < level) {
					dropLevel++;
				}
			}
		}
		System.out.println("After raise: " + raiseLevel / raiseSize);
		System.out.println("After drop: " + dropLevel / dropSize);
	}

	/**
	 * Analyse the price between the last close and the next high
	 * 
	 * @param symbol
	 * @param arrayHis
	 */

	public static void oneStockTwoDayCloseHighAnalyse(String symbol,
			String arrayHis) {
		JSONArray historyInfos = new JSONArray(arrayHis);
		int lengthRang = 0;
		if (PeriodRang > historyInfos.length() - 1) {
			lengthRang = historyInfos.length() - 1;
		} else {
			lengthRang = PeriodRang;
		}

		List<Double> OpenRates = new ArrayList<Double>();
		List<Double> HighRates = new ArrayList<Double>();
		List<Double> LowRates = new ArrayList<Double>();
		List<Double> CloseRates = new ArrayList<Double>();
		for (int i = lengthRang; i > 0; i--) {
			try {
				Double NextOpen = parseDouble(historyInfos.getJSONArray(i - 1)
						.getString(1));
				Double NextHigh = parseDouble(historyInfos.getJSONArray(i - 1)
						.getString(2));
				Double NextLow = parseDouble(historyInfos.getJSONArray(i - 1)
						.getString(3));
				Double NextClose = parseDouble(historyInfos.getJSONArray(i - 1)
						.getString(4));
				Double LastClose = parseDouble(historyInfos.getJSONArray(i)
						.getString(4));

				Double OpenRate = (NextOpen - LastClose) / LastClose;
				Double HighRate = (NextHigh - LastClose) / LastClose;
				Double LowRate = (NextLow - LastClose) / LastClose;
				Double CloseRate = (NextClose - LastClose) / LastClose;

				OpenRates.add(OpenRate);
				HighRates.add(HighRate);
				LowRates.add(LowRate);
				CloseRates.add(CloseRate);

			} catch (JSONException e) {
				// e.printStackTrace();
			} catch (Exception e) {
				// e.printStackTrace();
			}
		}
		Double lastPrice = 0.0;
		try {
			lastPrice = parseDouble(historyInfos.getJSONArray(0).getString(4));

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}

		try {

			if (System.currentTimeMillis()
					- new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(
							historyInfos.getJSONArray(0).getString(0))
							.getTime() > 433782878) {
				return;
			}

			if (Double.parseDouble(historyInfos.getJSONArray(0).getString(5))
					* Double.parseDouble(historyInfos.getJSONArray(0)
							.getString(4)) < leastEstCap) {
				return;
			}
		} catch (Exception e) {

		}

		oneStockTwoDayCloseHighReport(symbol, "low", LowRates, lastPrice,
				"oneStockTwoDayCloseHigh");
		oneStockTwoDayCloseHighReport(symbol, "high", HighRates, lastPrice,
				"oneStockTwoDayCloseHigh");
		oneStockTwoDayCloseHighReport(symbol, "open", OpenRates, lastPrice,
				"oneStockTwoDayCloseHigh");
		oneStockTwoDayCloseHighReport(symbol, "close", CloseRates, lastPrice,
				"oneStockTwoDayCloseHigh");
	}

	/**
	 * Report one day analyse
	 * 
	 * @param symbol
	 * @param type
	 * @param rates
	 * @param lastPrice
	 * @return
	 */
	public static boolean oneStockTwoDayCloseHighReport(String symbol,
			String type, List<Double> rates, Double lastPrice, String tableName) {
		if (rates.size() == 0) {
			return false;
		}
		Double Total = 0.0, Average = 0.0, HighestRate = 0.0, LowestRate = 0.0, ZERO = 0.0, ONE = 0.0, TWO = 0.0, THREE = 0.0, FOUR = 0.0, FIVE = 0.0, SIX = 0.0, OTHER = 0.0, Size = (double) rates
				.size(), MinusONE = 0.0, MinusTWO = 0.0, MinusTHREE = 0.0, MinusFOUR = 0.0, MinusFIVE = 0.0, MinusSIX = 0.0, MinusOTHER = 0.0;

		for (Double rate : rates) {
			Total += rate;
			int roundNum = new Double(rate * 100).intValue();
			if (rate > HighestRate) {
				HighestRate = rate;
			}
			if (rate < LowestRate) {
				LowestRate = rate;
			}
			switch (roundNum) {
			case 0:
				ZERO++;
				break;
			case 1:
				ONE++;
				break;
			case 2:
				TWO++;
				break;
			case 3:
				THREE++;
				break;
			case 4:
				FOUR++;
				break;
			case 5:
				FIVE++;
				break;
			case 6:
				SIX++;
				break;
			case -1:
				MinusONE++;
				break;
			case -2:
				MinusTWO++;
				break;
			case -3:
				MinusTHREE++;
				break;
			case -4:
				MinusFOUR++;
				break;
			case -5:
				MinusFIVE++;
				break;
			case -6:
				MinusSIX++;
				break;
			default:
				if (roundNum > 0) {
					OTHER++;
				} else {
					MinusOTHER++;
				}
				break;
			}
		}
		Average = Total / Size;
		Double standardDevition = getStandardDevition(rates, Average);

		Map<String, Double> dailyRates = new TreeMap<String, Double>();
		dailyRates.put("average", Average);
		dailyRates.put("highestRate", HighestRate);
		dailyRates.put("lowestRate", LowestRate);
		dailyRates.put("ZERO", ZERO / Size);
		dailyRates.put("ONE", ONE / Size);
		dailyRates.put("TWO", TWO / Size);
		dailyRates.put("THREE", THREE / Size);
		dailyRates.put("FOUR", FOUR / Size);
		dailyRates.put("FIVE", FIVE / Size);
		dailyRates.put("SIX", SIX / Size);
		dailyRates.put("MinusONE", MinusONE / Size);
		dailyRates.put("MinusTWO", MinusTWO / Size);
		dailyRates.put("MinusTHREE", MinusTHREE / Size);
		dailyRates.put("MinusFOUR", MinusFOUR / Size);
		dailyRates.put("MinusFIVE", MinusFIVE / Size);
		dailyRates.put("MinusSIX", MinusSIX / Size);
		dailyRates.put("MinusOther", MinusOTHER / Size);
		dailyRates.put("OTHER", OTHER / Size);
		dailyRates.put("standardDevition", standardDevition / Size);
		dailyRates.put("Size", Size);
		dailyRates.put("LastPrice", lastPrice);
		if (KongList.containsKey(symbol)) {
			dailyRates.put("Kong", KongList.get(symbol));
		} else {
			dailyRates.put("Kong", 1.0);
		}

		Map<String, String> StrPair = new HashMap<String, String>();
		StrPair.put("Symbol", symbol);
		StrPair.put("Type", type);
		// System.out.println("coming");
		if (!OneStockTwoDayTableCreated) {
			MySql.createNumberTable(MysqlDatabase, OneStockTwoDayTableName,
					dailyRates.keySet(), StrPair.keySet());
			OneStockTwoDayTableCreated = true;
		}

		MySql.insertNumberData(MysqlDatabase, OneStockTwoDayTableName,
				dailyRates, StrPair);
		return true;
	}

	/**
	 * Read maikong long and write in kong list
	 */
	public static void readMaikong() {
		File GroupFile = new File("maikong");
		if (GroupFile.exists()) {
			BufferedReader in;
			try {
				in = new BufferedReader(new FileReader(GroupFile));
				String line = in.readLine();
				while (line != null) {
					String[] items = line.split("	");
					KongList.put(items[0], Double.parseDouble(items[1]));
					line = in.readLine();
				}
				in.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else {
			System.out.println("No config file");
		}
	}

	public static void oneStockDropsAndRaisesAnalyse(String symbol,
			String arrayHis) {
		JSONArray historyInfos = new JSONArray(arrayHis);
		List<Double> twoCloseRates = new ArrayList<Double>();
		List<Double> HighCloseRates = new ArrayList<Double>();
		if (historyInfos.length() <= PeriodRang) {
			return;
		}
		for (int i = historyInfos.length() - 1; i > 0; i--) {
			try {
				Double NextOpen = parseDouble(historyInfos.getJSONArray(i - 1)
						.getString(1));
				Double NextHigh = parseDouble(historyInfos.getJSONArray(i - 1)
						.getString(2));
				Double NextLow = parseDouble(historyInfos.getJSONArray(i - 1)
						.getString(3));
				Double NextClose = parseDouble(historyInfos.getJSONArray(i - 1)
						.getString(4));
				Double LastClose = parseDouble(historyInfos.getJSONArray(i)
						.getString(4));
				Double TwoCloseRate = (NextClose - LastClose) / LastClose;
				Double HighCloseRate = (NextHigh - LastClose) / LastClose;
				twoCloseRates.add(TwoCloseRate);
				if (HighCloseRate < 0.5) {
					HighCloseRates.add(HighCloseRate);
				}

			} catch (JSONException e) {
				// e.printStackTrace();
			} catch (Exception e) {
				// e.printStackTrace();
			}
		}
		oneStockDropsAndRaisesReport(symbol, HighCloseRates, "high", true);
		oneStockContinousDropsAndRaisesReport(symbol, HighCloseRates, "high",
				true);
	}

	public static void oneStockContinousDropsAndRaisesReport(String symbol,
			List<Double> rates, String type, boolean raise) {

		for (int i = 0; i < rates.size(); i++) {
			int length = 0;
			boolean continueRun = true;
			while (continueRun) {
				if (raise) {
					if (rates.get(i) >= DropsAndRaisesRate) {
						length++;
						i++;
					} else {
						continueRun = false;
					}
				} else {
					if (rates.get(i) <= (-1 * DropsAndRaisesRate)) {
						length++;
						i++;
					} else {
						continueRun = false;
					}
				}
				if (i >= rates.size()) {
					break;
				}
			}
			if (length >= 3) {
				if (DropsAndRaisesFrequency.containsKey(length)) {
					DropsAndRaisesFrequency.put(length,
							DropsAndRaisesFrequency.get(length) + 1);
				} else {
					DropsAndRaisesFrequency.put(length, 1);
				}
			}
		}

	}

	public static void oneStockDropsAndRaisesReport(String symbol,
			List<Double> rates, String type, boolean raise) {
		// for (Double number : rates) {
		// System.out.print(number+" ");
		// }
		// System.out.println();
		Double YesSize = 0.0;
		Double NoSize = 0.0;
		for (int i = 0; i < rates.size(); i++) {
			boolean reachPoint = true;
			List<Double> numbers = new ArrayList<Double>();

			for (int j = 0; j < PeriodRang; j++) {
				i++;
				if (i >= rates.size()) {
					break;
				}

				if (raise) {
					if (rates.get(i) < DropsAndRaisesRate) {
						reachPoint = false;
						break;
					}
				} else {
					if (rates.get(i) > (-1 * DropsAndRaisesRate)) {
						reachPoint = false;
						break;
					}
				}
				numbers.add(rates.get(i));
			}
			if (i >= rates.size()) {
				break;
			}
			if (reachPoint == false) {
				continue;
			}
			i++;
			if (i >= rates.size()) {
				break;
			}
			numbers.add(rates.get(i));
			for (Double number : numbers) {
				// System.out.print(String.valueOf(number) + "   ");
			}
			if (raise) {
				if (rates.get(i) >= DropsAndRaisesRate) {
					YesSize++;
					numbers.add(1.0);
					// System.out.print(" Yes\r\n");
				} else {
					NoSize++;
					numbers.add(0.0);
					// System.out.print(" No\r\n");
				}
			} else {
				if (rates.get(i) <= (-1 * DropsAndRaisesRate)) {
					YesSize++;
					numbers.add(1.0);
					// System.out.print(" Yes\r\n");
				} else {
					NoSize++;
					numbers.add(0.0);
					// System.out.print(" No\r\n");
				}
			}
			NumberRates.add(numbers);
		}
		if (YesSize + NoSize != 0) {
			Double rate = YesSize / (YesSize + NoSize);
			// System.out.println("Rate:" + rate);
		}
	}

	
	/**
	 * Report Drop And Raise rate
	 */
	public static void analyseDropAndRaise() {

		Double YesSize = 0.0;
		Double TotalNum = 0.0;
		for (List<Double> number : NumberRates) {
			if (number.get(number.size() - 1) == 1.0) {
				YesSize++;
			}
			TotalNum += number.get(number.size() - 2);
		}
		Double YesRate = YesSize / new Double(NumberRates.size());
		Double everage = TotalNum / NumberRates.size();
		System.out.println("YesRate: " + YesRate);
		System.out.println("everage: " + everage);

		// for (Entry<Integer, Integer> entry :
		// DropsAndRaisesFrequency.entrySet()) {
		// System.out.println(entry.getKey()+"  "+entry.getValue());
		// }
		// oneStockDropRiseReport();
	}
}
