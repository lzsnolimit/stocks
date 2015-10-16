package Stock;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class QueryStocksHistory extends BaseQuery {

	private static List<String> symbols;
	private static List<String> errors = new ArrayList<String>();
	private static boolean handleError = true;
	private static String type;
	private static String length;
	private static String tablename = "StocksHisShort"; // or StocksHisShort
	private static int currentPosition;

	static {
		Hbase.setStrings(tablename, "all");
		Hbase.createTable(true);
	}

	/**
	 * Multiple threads interface
	 */
	public void run() {
		String symbolStr;
		while (true) {
			synchronized (QueryStocksHistory.class) {
				currentPosition--;
				if (currentPosition < 0) {
					return;
				}
				symbolStr = symbols.get(currentPosition);
			}
			// if (type.equals("US")) {
			// parseUSSymbols(symbolStr);
			// } else {
			// parseCNSymbols(symbolStr);
			// }

			parseCNSymbols(symbolStr);

		}
	}

	public static void handleErrors() {
		handleError = false;
		for (String symbol : errors) {
			// if (type.equals("US")) {
			// parseNASDAQSymbols(symbol);
			// } else {
			// parseCNSymbols(symbol);
			// }
			parseCNSymbols(symbol);
		}

		handleError = true;
		errors.clear();
	}

	/**
	 * Get all stocks in the same exchanger
	 * 
	 * @param type
	 */
	public static void GetStocksSymbol(String type) {
		Scan scan = new Scan();
		Filter ColumnFilter = new QualifierFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("20150806195250")));
		scan.setFilter(ColumnFilter);
		Hbase.setStrings("Stocks", "list");
		ResultScanner results = Hbase.getAllData(scan);
		BufferedWriter out = null;
		File LogFile = new File(type + "Stocks");
		try {
			out = new BufferedWriter(new FileWriter(LogFile, false));
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (Result result : results) {
			for (Cell cell : result.rawCells()) {
				try {
					JSONObject info = new JSONObject(new String(
							CellUtil.cloneValue(cell)));
					if (info.getString("exchange").equals(type)) {
						out.write(new String(CellUtil.cloneRow(cell)) + "\r\n");
					}
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
		try {
			out.flush(); // 把缓存区内容压入文件
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Read all symbols from file
	 * 
	 * @param isErrors
	 */
	public static List<String> readSymbols(String filename) {
		File symbolFile = new File(filename);
		List<String> symbolStrs = new ArrayList<String>();
		try {
			BufferedReader in = new BufferedReader(new FileReader(symbolFile));
			String line = in.readLine();
			while (line != null) {
				symbolStrs.add(line);
				line = in.readLine();
			}
			in.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return symbolStrs;
	}

	/**
	 * Parse nasdq page and write in hbase
	 * 
	 * @param symbol
	 */
	public static void parseUSSymbols(String symbol) {
		if (!Hbase.getData(symbol).equals("")) {
			// System.out.println(symbol + " Exists!");
			return;
		}
		String result = HttpRequest.sendPost("http://www.nasdaq.com/symbol/"
				+ symbol.toLowerCase() + "/historical", length + "|false|"
				+ symbol);
		if (result.equals("")) {
			WriteError(symbol);
			System.out.println(symbol + " result error");
			return;
		}
		// System.out.println(result);
		Document doc = Jsoup.parse(result);
		JSONArray HistoricalData = new JSONArray();
		try {
			Element body = doc.getElementsByTag("tbody").get(0);
			// System.out.println(body.toString());
			Elements nodes = body.getElementsByTag("tr");
			if (nodes.size() == 0) {
				WriteError(symbol);
				System.out.println(symbol + " size 0");
				return;
			}
			// System.out.println(nodes.size());
			for (Element node : nodes) {
				JSONArray DailyData = new JSONArray();
				Elements units = node.getElementsByTag("td");
				for (Element unit : units) {
					if (!unit.text().equals("")) {
						DailyData.put(unit.text());
					}

				}
				if (DailyData.length() > 0) {
					HistoricalData.put(DailyData);
				}
			}
			Hbase.addData(symbol, type, HistoricalData.toString());
			// System.out.println(symbol + " done");
		} catch (Exception e) {
			if (handleError) {
				errors.add(symbol);
			} else {
				WriteError(symbol);
				System.out.println(symbol + " parsing error");
			}

			// TODO: handle exception
		}
	}

	/**
	 * Parse cn page and write in hbase
	 * 
	 * @param symbol
	 */
	public static void parseCNSymbols(String symbol) {
		if (!Hbase.getData(symbol).equals("")) {
			// System.out.println(symbol + " Exists!");
			return;
		}
		String url = "http://xueqiu.com/S/" + symbol + "/historical.csv";
		Response rs = null;
		// System.out.println(url);
		try {
			Connection con = getConnection(url, "historyHttp");
			con.header("Referer", " http://xueqiu.com/S/" + symbol);
			rs = con.execute();
			// System.out.println(rs.body());
		} catch (IOException e1) {
			if (handleError) {
				System.out.println(symbol + " http error");
				errors.add(symbol);
			} else {
				WriteError(symbol);
				System.out.println(symbol + " http error");
			}
			return;
		}

		try {
			BufferedReader reader = new BufferedReader(new StringReader(
					rs.body()));// 换成你的文件名
			reader.readLine();// 第一行信息，为标题信息，不用，如果需要，注释掉
			String line = null;
			JSONArray HistoricalData = new JSONArray();
			List<JSONArray> jsonLists = new ArrayList<JSONArray>();
			while ((line = reader.readLine()) != null) {
				String item[] = line.split(",");// CSV格式文件为逗号分隔符文件，这里根据逗号切分
				// System.out.println(item[0]);
				JSONArray DailyData = new JSONArray();
				for (int i = 1; i < item.length; i++) {
					item[i] = item[i].replace("\"", "");
					DailyData.put(item[i]);
				}
				if (Double.valueOf(DailyData.getString(2)) != 0) {
					jsonLists.add(DailyData);
				}
			}
			for (int i = (jsonLists.size() - 1); i >= 0; i--) {
				HistoricalData.put(jsonLists.get(i));
			}
			Hbase.addData(symbol, type, HistoricalData.toString());
			// System.out.println(symbol + " done");
			// System.out.println(jsonLists);
		} catch (Exception e) {

			if (handleError) {
				System.out.println(symbol + " parsing error");
				errors.add(symbol);
			} else {
				WriteError(symbol);
				System.out.println(symbol + " parsing error");
			}
		}
	}

	/**
	 * Write error in file
	 * 
	 * @param symbol
	 */
	public static void WriteError(String symbol) {
		File LogFile = new File("errors");
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(LogFile,
					true));
			out.write(symbol + "\r\n");
			out.flush(); // 把缓存区内容压入文件
			out.close(); // 最后记得关闭文件
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Start multiple threads
	 * 
	 * @param size
	 *            of threads
	 */
	public static void startThreads(int size, String hisLength) {
		QueryStocksHistory[] threads;

		type = "CN";
		symbols = readSymbols("CNSymbols");
		currentPosition = symbols.size() - 1;
		threads = new QueryStocksHistory[size];
		for (int i = 0; i < size; i++) {
			threads[i] = new QueryStocksHistory();
			threads[i].start();
		}
		for (int i = 0; i < size; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// handleErrors();

//		type = "US";
//		symbols = readSymbols("USSymbols");
//		currentPosition = symbols.size() - 1;
//		threads = new QueryStocksHistory[size];
//		for (int i = 0; i < size; i++) {
//			threads[i] = new QueryStocksHistory();
//			threads[i].start();
//		}
//		for (int i = 0; i < size; i++) {
//			try {
//				threads[i].join();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}

		// try {
		// AnalyseHistory.start("US");
		// AnalyseHistory.start("CN");
		// } catch (ClassNotFoundException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (InterruptedException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		// handleErrors();
	}

}
