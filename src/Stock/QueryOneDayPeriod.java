package Stock;

import java.io.IOException;
import java.util.List;

import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Connection.Response;

public class QueryOneDayPeriod extends BaseQuery {

	private static List<String> symbols;
	private static String type;
	private static String tablename = "StockOneDayPeriod";
	private static int currentPosition;
	static {
		Hbase.setStrings(tablename, "all");
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
			parseSymbol(symbolStr);

		}
	}

	/**
	 * Parse one page and write in hbase
	 * 
	 * @param symbol
	 */
	public static void parseSymbol(String symbol) {
		if (!Hbase.getData(symbol).equals("")) {
			// System.out.println(symbol + " Exists!");
			return;
		}
//		String url = "http://xueqiu.com/stock/forchartk/stocklist.json?symbol="
//				+ symbol + "&period=1day&type=normal&begin=1262278800000&end="
//				+ System.currentTimeMillis() + "+&_="
//				+ System.currentTimeMillis();
		String url = "http://xueqiu.com/stock/forchartk/stocklist.json?symbol="
				+ symbol + "&period=1day&type=normal&begin=1262278800000&end="
				+ System.currentTimeMillis() + "+&_="
				+ System.currentTimeMillis();
		Response rs = null;

		try {
			Connection con = getConnection(url, "oneDayPeriodHttp");
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
			Hbase.addData(symbol, type, data);
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
	public static void startThreads(int size, Boolean replace, String symbolType) {
		Hbase.createTable(replace);
		QueryOneDayPeriod[] threads;
		symbols = readSymbols(symbolType + "Symbols");
		currentPosition = symbols.size() - 1;
		type = symbolType + "OneDayPeriod";

		// System.out.println(times);
		size--;
		threads = new QueryOneDayPeriod[size];
		for (int i = 0; i < size; i++) {
			threads[i] = new QueryOneDayPeriod();
			threads[i].start();
		}
		for (int i = 0; i < size; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// type = "CNOneDayPeriod";
		// symbols = readSymbols("CNSymbols");
		// currentPosition = symbols.size() - 1;
		// threads = new QueryOneDayPeriod[size];
		// for (int i = 0; i < size; i++) {
		// threads[i] = new QueryOneDayPeriod();
		// threads[i].start();
		// }
		// for (int i = 0; i < size; i++) {
		// try {
		// threads[i].join();
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// }
		// type = "USOneDayPeriod";
		// symbols = readSymbols("USSymbols");
		// currentPosition = symbols.size() - 1;
		// threads = new QueryOneDayPeriod[size];
		// for (int i = 0; i < size; i++) {
		// threads[i] = new QueryOneDayPeriod();
		// threads[i].start();
		// }
		// for (int i = 0; i < size; i++) {
		// try {
		// threads[i].join();
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// }
		// type = "HKOneDayPeriod";
		// symbols = readSymbols("HKSymbols");
		// currentPosition = symbols.size() - 1;
		// threads = new QueryOneDayPeriod[size];
		// for (int i = 0; i < size; i++) {
		// threads[i] = new QueryOneDayPeriod();
		// threads[i].start();
		// }
		// for (int i = 0; i < size; i++) {
		// try {
		// threads[i].join();
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// }
	}
}
