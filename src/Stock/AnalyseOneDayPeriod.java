package Stock;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

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
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Connection.Response;

public class AnalyseOneDayPeriod extends BaseAnalyse {
	private static String HbaseTableName = "StockOneDayPeriod";
	public static int PeriodRang = 5;
	public static Double baseRate = 0.06;
	public static Double baseLowRate = -0.02;

	public static Map<Integer, List<Double>> resultRates;
	public static Map<String, List<String>> resultSymbol;

	// public static JSONObject ratePossiblity=new JSONObject();

	public static class MyMapper extends TableMapper<Text, DoubleWritable> {
		/**
		 * map method
		 */
		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {
//			analysisRate(new String(CellUtil.cloneRow(value.rawCells()[0])),
//					new String(CellUtil.cloneValue(value.rawCells()[0])));

			getSymbols(new String(CellUtil.cloneRow(value.rawCells()[0])),
					new String(CellUtil.cloneValue(value.rawCells()[0])));

			// getZhuTuSymbols(new
			// String(CellUtil.cloneRow(value.rawCells()[0])),
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
	public static void start(String type) throws IOException,
			ClassNotFoundException, InterruptedException {
		resultRates = new TreeMap<Integer, List<Double>>();
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "StockSummary");
		job.setJarByClass(App.class); // class that contains mapper and reducer
		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		Filter ColumnFilter = new QualifierFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(type + "OneDayPeriod")));
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
		if (!b) {
			throw new IOException("error with job!");
		}
		// analyseResult();
	}

	/**
	 * Analysis one day period
	 * 
	 * @param symbol
	 * @param HisArray
	 */
	public static void analysisRate(String symbol, String HisArray) {
		JSONArray historyInfos = new JSONArray(HisArray);
		if (PeriodRang > historyInfos.length() - 1) {
			return;
		}
		List<Double> CloseHighRates = getRate(symbol, "close", "high",
				historyInfos, 1);
		int currentPosition = 0;
		while (currentPosition < historyInfos.length() - PeriodRang) {
			Map<String, Double> result = judgeContinousRange(CloseHighRates,
					baseRate, "larger", currentPosition);
			int length = result.get("size").intValue();
			double everage = result.get("everage");
			if (length == 0) {
				currentPosition++;
			} else if (length < PeriodRang) {
				currentPosition += length;
			} else {
				analyseRateWrite(historyInfos, length, currentPosition);
				currentPosition += length + 1;
			}
		}
	}

	/**
	 * Write data
	 * 
	 * @param historyInfos
	 * @param length
	 * @param startPosition
	 */
	public static void analyseRateWrite(JSONArray historyInfos, int length,
			int startPosition) {
		JSONArray results = new JSONArray();
		try {
			for (int i = startPosition; i <= startPosition + length + 1; i++) {
				results.put(historyInfos.getJSONObject(i));
			}
			writeFile("results/onedayperiod/highclose/" + baseRate,
					results.toString(), true);
		} catch (Exception e) {
		}

	}

	/**
	 * Get the symbols fit the situation
	 * 
	 * @param symbol
	 * @param HisArray
	 */
	public static void getSymbols(String symbol, String HisArray) {
		JSONArray historyInfos = new JSONArray(HisArray);
		List<Double> CloseHighRates = getRate(symbol, "close", "high",
				historyInfos, 1);
		List<Double> CloseLowRates = getRate(symbol, "close", "low",
				historyInfos, 1);
		Map<String, Double> resultrate = getLastContinousRange(CloseHighRates,
				baseRate, "larger");
		Map<String, Double> LowResultRate = getLastContinousRange(
				CloseLowRates, -baseRate, "larger");

		if (resultrate.get("size") >= PeriodRang) {
			double volume = historyInfos.getJSONObject(
					historyInfos.length() - 1).getDouble("volume");
			double close = historyInfos
					.getJSONObject(historyInfos.length() - 1)
					.getDouble("close");
			if (volume * close > 1000000 && close >= 1) {
				try {
					String dateStr = historyInfos.getJSONObject(
							historyInfos.length() - 1).getString("time");
					if ((System.currentTimeMillis() - new java.text.SimpleDateFormat(
							"EEE MMM dd HH:mm:ss zzzzz yyyy").parse(dateStr)
							.getTime()) < 15 * 1000 * 24 * 60 * 60) {
						// resultSymbol.put(symbol, resultrate.get("size"));
						List<String> ResultList = new ArrayList<String>();
						JSONObject details = new JSONObject(readToday(symbol));
						Double last_close = Double.parseDouble(details
								.getString("last_close"));
						Double high = Double.parseDouble(details
								.getString("high"));
						Double low = Double.parseDouble(details
								.getString("low"));
						Double highRate = high / last_close - 1;
						Double lowRate = low / last_close - 1;
						// System.out.println(symbol+" "+highRate+"  "+high+"  "+last_close+"  "+baseRate);
						if (highRate >= baseRate && lowRate > -baseRate) {
							ResultList.add(String.valueOf(resultrate
									.get("size") + 1));
							ResultList.add(String.valueOf(LowResultRate
									.get("size") + 1));
							resultSymbol.put(symbol, ResultList);
						}
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Get the symbols fit the zhutu situation
	 * 
	 * @param symbol
	 * @param HisArray
	 */
	public static void getZhuTuSymbols(String symbol, String HisArray) {
		JSONArray historyInfos = new JSONArray(HisArray);
		List<Double> CloseHighRates = getRate(symbol, "close", "high",
				historyInfos, 1);
		List<Double> closeCloseRates = getRate(symbol, "close", "close",
				historyInfos, 1);
		Map<String, Integer> resultrate = getLastContinousZhutuRange(
				closeCloseRates, CloseHighRates, baseRate, baseRate / 2);

		if (resultrate.get("size") >= PeriodRang) {
			double volume = historyInfos.getJSONObject(
					historyInfos.length() - 1).getDouble("volume");
			double close = historyInfos
					.getJSONObject(historyInfos.length() - 1)
					.getDouble("close");

			if (volume * close > 100000 && close >= 1) {

				try {
					String dateStr = historyInfos.getJSONObject(
							historyInfos.length() - 1).getString("time");

					if ((System.currentTimeMillis() - new java.text.SimpleDateFormat(
							"EEE MMM dd HH:mm:ss zzzzz yyyy").parse(dateStr)
							.getTime()) < 10 * 1000 * 24 * 60 * 60) {
						// resultSymbol.put(symbol, resultrate.get("size"));

						List<String> ResultList = new ArrayList<String>();
						JSONObject details = new JSONObject(readToday(symbol));
						Double last_close = Double.parseDouble(details
								.getString("last_close"));
						Double high = Double.parseDouble(details
								.getString("high"));
						Double highRate = high / last_close - 1;
						if (highRate >= baseRate) {
							ResultList.add(String.valueOf(resultrate
									.get("size") + 1));
						}
					}

				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

		}
	}

	/**
	 * Analyse result
	 */

	public static void analyseResult() {
		for (Entry<Integer, List<Double>> pair : resultRates.entrySet()) {
			analyseList(pair.getValue(), pair.getKey());
		}
	}

	public static void analyseList(List<Double> rates, int number) {
		System.out.println("Number size: " + rates.size());
		int aboveBase1Size = 0, aboveBase2Size = 0;
		Double aboveRate1 = 0.03, aboveRate2 = 0.04;
		System.out.println("Base rate: " + baseRate + "  Range: " + number);

		for (Double num : rates) {
			if (num > aboveRate1) {
				aboveBase1Size++;
			}
			if (num > aboveRate2) {
				aboveBase2Size++;
			}
		}

		System.out.println("Above 0.03 Rate: " + new Double(aboveBase1Size)
				/ new Double(rates.size()));
		System.out.println("Above 0.04 Rate: " + new Double(aboveBase2Size)
				/ new Double(rates.size()));
	}

	/**
	 * start analyse with rate range
	 * 
	 * @param startRate
	 * @param endRate
	 * @param symType
	 */

	public static void startWithRateRange(Double startRate, Double endRate,
			String symType) {
		for (double currentRate = startRate; currentRate <= endRate; currentRate += 0.01) {

			resultSymbol = new HashMap<String, List<String>>();
			try {
				baseRate = currentRate;
				start(symType);
				List<Entry<String, List<String>>> list = new ArrayList<Entry<String, List<String>>>(
						resultSymbol.entrySet());
				Collections.sort(list,
						new Comparator<Map.Entry<String, List<String>>>() {
							public int compare(
									Map.Entry<String, List<String>> o1,
									Map.Entry<String, List<String>> o2) {
								return (int) (Double.parseDouble(o2.getValue()
										.get(0)) - Double.parseDouble(o1
										.getValue().get(0)));
							}
						});
				System.out.print("\n" + currentRate + "\n");
				for (Entry<String, List<String>> pair : list) {
					System.out.print(pair.getKey() + "  ");
					for (String str : pair.getValue()) {
						System.out.print(str + "  ");
					}
				}
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * show results with everage
	 * 
	 * @param symType
	 */

	public static void startWithEverage(String symType) {

	}

	/**
	 * Read detail
	 * http://xueqiu.com/v4/stock/quote.json?code=SFUN&_=1435809669885
	 * 
	 * @param symbol
	 */
	public static String readToday(String symbol) {
		// System.out.println(symbol);
		String url = "http://xueqiu.com/v4/stock/quote.json?code=" + symbol
				+ "&_=" + System.currentTimeMillis();
		Response rs = null;
		try {
			Connection con = BaseQuery.getConnection(url, "AllStocksHttp");
			con.header("Referer", " http://xueqiu.com/S/" + symbol);
			rs = con.execute();
			JSONObject jObject = new JSONObject(rs.body());
			jObject = jObject.getJSONObject(jObject.keys().next());
			return jObject.toString();
		} catch (Exception e) {
			// e.printStackTrace();
			// System.out.println(symbol + " Parse error!");
			return readToday(symbol);
		}
	}

	/**
	 * Get rate between two days,
	 * 
	 * @param symbol
	 * @param baseNumName
	 *            The base name
	 * @param DivideNumName
	 *            The divide name
	 * @param jsonDate
	 *            history array
	 * @param dayFif
	 *            if it the the same day dayFif=0
	 * @return
	 */
	public static List<Double> getRate(String symbol, String baseNumName,
			String DivideNumName, JSONArray jsonDate, Integer dayDif) {
		List<Double> rates = new ArrayList<Double>();

		// System.out.println(jsonDate.toString());
		for (int i = 0; i < jsonDate.length() - Math.abs(dayDif); i++) {
			Double baseNum = 0.0, divNum = 0.0;
			try {
				baseNum = jsonDate.getJSONObject(i).getDouble(baseNumName);
				divNum = jsonDate.getJSONObject(i + dayDif).getDouble(
						DivideNumName);
			} catch (Exception e) {
				baseNum = Double.parseDouble(jsonDate.getJSONObject(i)
						.getString(baseNumName));
				divNum = Double.parseDouble(jsonDate.getJSONObject(i + dayDif)
						.getString(DivideNumName));
				// TODO: handle exception
			}
			if (!Double.isNaN(divNum / baseNum - 1)) {
				// System.out.print(divNum / baseNum - 1+"  ");
				rates.add(divNum / baseNum - 1);
			}
		}

		// for (int i = 0; i < rates.size(); i++) {
		// System.out.print(rates.get(i) + "  ");
		// }

		// for (Double num : rates) {
		// System.out.print(num + "  ");
		// if (num.isNaN()) {
		// System.out.print(num + "  ");
		// }
		// }
		return rates;
	}

	/**
	 * Get last continuous range
	 * 
	 * @param rates
	 * @param startRate
	 * @param triger
	 *            larger smaller or equal
	 * @return Map<String, Double>
	 */
	public static Map<String, Double> getLastContinousRange(List<Double> rates,
			Double startRate, String triger) {
		boolean continueRun = true;
		Integer size = 0;
		Double total = 0.0;
		// System.out.println(rates.size());
		for (int i = rates.size() - 1; i >= 0; i--) {
			switch (triger) {
			case "larger":
				if (rates.get(i) < startRate) {
					continueRun = false;
				} else {
					total += rates.get(i);
					size++;
				}
				break;
			case "smaller":
				if (rates.get(i) > startRate) {
					continueRun = false;
				} else {
					total += rates.get(i);
					size++;
				}
				break;
			case "equal":

				if (rates.get(i) != startRate) {
					continueRun = false;
				} else {
					total += rates.get(i);
					size++;
				}
				break;

			default:
				break;
			}
			if (continueRun == false) {
				break;
			}
		}

		Map<String, Double> results = new HashMap<String, Double>();
		results.put("size", new Double(size));
		results.put("everage", total / size);
		// System.out.println("SIZE: "+size);
		// System.out.println("Everage: "+ total/size);
		return results;
	}

	/**
	 * Judge continuous range
	 * 
	 * @param rates
	 * @param startRate
	 * @param triger
	 *            larger smaller or equal
	 * @param start
	 * @param range
	 * @return boolean
	 */
	public static Map<String, Double> judgeContinousRange(List<Double> rates,
			Double startRate, String triger, int start) {
		// System.out.println();
		Double size = 0.0;
		Map<String, Double> result = new HashMap<String, Double>();
		Double total = 0.0;
		for (int i = start; i < rates.size(); i++) {
			switch (triger) {
			case "larger":
				if (rates.get(i) < startRate) {
					result.put("size", size);
					result.put("everage", total / size);
					return result;
					// isYes = false;
				}
				break;
			case "smaller":
				if (rates.get(i) > startRate) {
					result.put("size", size);
					result.put("everage", total / size);
					return result;
					// isYes = false;
				}
				break;
			case "equal":
				if (rates.get(i) != startRate) {
					result.put("size", size);
					result.put("everage", total / size);
					return result;
					// isYes = false;
				}
				break;
			default:
				return result;
			}
			// System.out.print(rates.get(i)+" ");
			size++;
			total += rates.get(i);
		}
		// if (size != 0) {
		// System.out.println("\n");
		// for (int i = start; i < start + size; i++) {
		// System.out.print(rates.get(i) + "  ");
		// }
		// System.out.println("\n");
		// }
		result.put("size", size);
		result.put("everage", total / size);
		return result;
	}

	/**
	 * Get last continuous Zhutu range
	 * 
	 * @param rates
	 * @param startRate
	 * @param triger
	 *            larger smaller or equal
	 * @return Map<String, Double>
	 */
	public static Map<String, Integer> getLastContinousZhutuRange(
			List<Double> closeCloseRates, List<Double> closeHighRates,
			Double highRate, Double closeRate) {
		boolean continueRun = true;
		Integer size = 0;
		Double total = 0.0;
		// System.out.println(rates.size());
		for (int i = closeHighRates.size() - 1; i >= 0; i--) {

			if (closeHighRates.get(i) < highRate
					|| closeCloseRates.get(i) > closeRate) {
				continueRun = false;
			} else {
				total += closeHighRates.get(i);
				size++;
			}
			if (continueRun == false) {
				break;
			}
		}

		Map<String, Integer> results = new HashMap<String, Integer>();
		results.put("size", size);
		// System.out.println("SIZE: "+size);
		// System.out.println("Everage: "+ total/size);
		return results;
	}
}
