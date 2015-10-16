package Stock;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Connection.Response;

public class queryPricesInOneDay extends BaseQuery {
	public static List<String> errors = new ArrayList<String>();
	public static List<String> symbols;
	public static Vector<Map<String, Float>> groups = new Vector<Map<String, Float>>();
	public static String currentTime = new java.text.SimpleDateFormat(
			"yyyy-MM-dd").format(new Date(System.currentTimeMillis()));
	public static boolean handleError = true;
	public static String HbaseTable = "StocksPrice";
	public static int currentPosition;
	public static String queryType;
	static {
		Vector<String> familyNames = new Vector<String>();
		familyNames.add("US");
		familyNames.add("CN");
		Hbase.setStrings(HbaseTable, "");
		Hbase.createTable(false, familyNames, HbaseTable);
	}

	/**
	 * multiple threads interface
	 */
	public void run() {
		String symbol;
		while (true) {
			synchronized (queryAllStocks.class) {
				currentPosition--;
				if (currentPosition < 0) {
					return;
				}
				symbol = symbols.get(currentPosition);
			}
			parseOneStockFrom(symbol, queryType);
		}

	}

	/**
	 * Parse one stock
	 * 
	 * @param symbol
	 * @param type
	 */
	public static void parseOneStockFrom(String symbol, String type) {
		if (!Hbase.getDataWithColumn(symbol, currentTime).equals("")) {
			return;
		}
		String url = "http://xueqiu.com/stock/forchart/stocklist.json?symbol="
				+ symbol + "&period=1d&one_min=1&_="
				+ System.currentTimeMillis();
		Response rs = null;
		Connection con;

		try {
			con = BaseQuery.getConnection(url, "PriceByMinuteHttp");
			con.header("Referer", "Referer:http://xueqiu.com/S/" + symbol);
			rs = con.execute();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			
			System.out.println(symbol + "Http error");
			if (handleError) {
				errors.add(symbol);
			}

		}

		try {
			JSONObject jObject = new JSONObject(rs.body());
			Hbase.addData(symbol, currentTime, jObject.get("chartlist")
					.toString(), type, HbaseTable);
		} catch (Exception e) {
			System.out.println(symbol + "Parsing error");
			System.out.println(rs.body());
			if (handleError) {
				errors.add(symbol);
			}
		}
	}

	/**
	 * Handle error
	 * 
	 * @param type
	 */
	public static void HandleErrors() {
		handleError = false;
		for (String error : errors) {
			parseOneStockFrom(error, queryType);
		}
		errors.clear();
		handleError = true;
	}

	/**
	 * Read file
	 * 
	 * @param fileNames
	 * @param lists
	 */
	public static List<String> readFile(String fileNames) {
		File LogFile = new File("errors");
		List<String> lists = new ArrayList<String>();
		String line;
		try {
			BufferedReader in = new BufferedReader(new FileReader(fileNames));
			while ((line = in.readLine()) != null) {
				lists.add(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lists;
	}

	/**
	 * Write file
	 * 
	 * @param fileNames
	 * @param value
	 * @param append
	 */
	public static void writeFile(String fileNames, String value, Boolean append) {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(fileNames,
					append));
			out.write(value + "\r\n");
			out.flush();
			out.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Write file
	 * 
	 * @param fileNames
	 * @param vlues
	 * @param append
	 */
	public static void writeFile(String fileNames, List<String> values,
			Boolean append) {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(fileNames,
					append));
			for (String value : values) {
				out.write(value);
			}
			out.flush();
			out.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Start threads
	 * 
	 * @param size
	 * @param type
	 */
	public static void startThreads(int size, String type) {
		queryType = type;
		symbols = readFile(type + "StocksSymbol");
		currentPosition = symbols.size() - 1;
		queryPricesInOneDay[] threads = new queryPricesInOneDay[size];
		for (int i = 0; i < size; i++) {
			threads[i] = new queryPricesInOneDay();
			threads[i].start();
		}
		for (int i = 0; i < size; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		HandleErrors();
		writeFile("PriceInOneDayConfig", currentTime, true);
	}
}
