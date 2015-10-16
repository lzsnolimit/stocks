package Stock;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Connection.Response;

public class QueryFiveDaysPrice extends BaseQuery {

	/**
	 * Analyse US Five Days Price page
	 * 
	 * @param symbol
	 */
	public static final String HbaseTableName = "StocksFiveDaysPrice";
	public static List<String> symbols;
	public static String type;
	public static int currentPosition;
	public static String currentTime = new java.text.SimpleDateFormat(
			"yyyy-MM-dd").format(new Date(System.currentTimeMillis()));
	static {
		Hbase.setStrings(HbaseTableName, "all");
	}
	public void run() {
		String symbolStr;
		while (true) {
			synchronized (QueryFiveDaysPrice.class) {
				currentPosition--;
				if (currentPosition < 0) {
					return;
				}
				symbolStr = symbols.get(currentPosition);
			}
			parseXueQiuFiveDaysPrice(symbolStr);
		}
		
	}

	/**
	 * Parse xueqiu five days price
	 * 
	 * @param symbol
	 *            http://xueqiu.com/stock/forchart/stocklist.json?symbol=
	 *            SZ002442&period=5d&_=1443079801713
	 */
	public static void parseXueQiuFiveDaysPrice(String symbol) {
		if (!Hbase.getDataWithColumn(symbol, type + currentTime).equals("")) {
			//System.out.println(symbol + " Exists!");
			return;
		}
		String url = "http://xueqiu.com/stock/forchart/stocklist.json?symbol="
				+ symbol + "&period=5d&_=" + System.currentTimeMillis();
		Response rs = null;

		try {
			Connection con = getConnection(url, "FiveDaysHttp");
			con.header("Referer", " http://xueqiu.com/S/" + symbol);
			rs = con.execute();
			// System.out.println(rs.body());
		} catch (IOException e1) {
			System.out.println(symbol + " http error");
			return;
		}

		try {
			JSONObject oneDayObj = new JSONObject(rs.body());
			String data = oneDayObj.get("chartlist").toString();
			if (data == null || data.equals("null")) {
				// System.out.println(symbol + " Null");
				return;
			}
			//System.out.println(data);
			Hbase.addData(symbol, type + currentTime, data);
		} catch (Exception e) {
			System.out.println(symbol + " parsing error");
			// System.out.println(rs.body());
			return;
		}
	}

	/**
	 * Start multiple threads
	 * 
	 * @param size
	 *            of threads
	 */
	public static void startThreads(int size, String type,boolean replace) {
		Hbase.createTable(replace);
		QueryFiveDaysPrice[] threads;
		symbols = readSymbols(type + "Symbols");
		currentPosition = symbols.size() - 1;
		threads = new QueryFiveDaysPrice[size];
		for (int i = 0; i < size; i++) {
			threads[i] = new QueryFiveDaysPrice();
			threads[i].start();
		}
		for (int i = 0; i < size; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				// e.printStackTrace();
			}
		}
	}
}
