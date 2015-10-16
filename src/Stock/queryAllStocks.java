package Stock;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * 
 * @author Zhongshan Lu
 * 
 *         Scratch data from sina xueqiu
 *
 */
public class queryAllStocks extends BaseQuery {
	private static int currentPosition = 0;
	private static ArrayList<String> symbolList = new ArrayList<String>();
	private static ArrayList<String> errors = new ArrayList<String>();
	public static String currentTime = new java.text.SimpleDateFormat(
			"yyyy-MM-dd").format(new Date(System.currentTimeMillis()));
	private static boolean tableCreated = false;
	public static boolean handleError = true;
	/**
	 * Set and create Hbase
	 */
	static {
		Hbase.setStrings("Stocks", "list");
		Hbase.createTable(false);
		try {
			readListFromFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Threads interface
	 */

	@Override
	public void run() {
		String symbolStr;
		while (true) {
			synchronized (queryAllStocks.class) {
				currentPosition--;
				if (currentPosition < 0) {
					return;
				}
				symbolStr = symbolList.get(currentPosition);
			}
			readDetail(symbolStr, true);
		}
	}

	/**
	 * Handle errors with single thread
	 */

	public static void handleErrors() {
		for (String symbol : errors) {
			readDetail(symbol, false);
		}
	}

	/**
	 * Read detail and save in hbase
	 * http://xueqiu.com/v4/stock/quote.json?code=SFUN&_=1435809669885
	 * 
	 * @param symbol
	 * @param handleError
	 */
	public static void readDetail(String symbol, boolean handleError) {
		if (!Hbase.getDataWithColumn(symbol, currentTime).equals("")) {
			return;
		}
		// System.out.println(symbol);
		String url = "http://xueqiu.com/v4/stock/quote.json?code=" + symbol
				+ "&_=" + System.currentTimeMillis();
		Response rs = null;
		try {
			Connection con = getConnection(url, "AllStocksHttp");
			con.header("Referer", " http://xueqiu.com/S/" + symbol);
			rs = con.execute();
			JSONObject jObject = new JSONObject(rs.body());
			jObject = jObject.getJSONObject(jObject.keys().next());
			jObject.put("change_text", jObject.getString("change"));
			jObject.remove("change");
			synchronized (queryAllStocks.class) {
				if (!tableCreated) {
					MySql.createTable("stocks", "stock" + currentTime,
							jObject.keySet());
					tableCreated = true;
				}
			}
			//MySql.insertData("stocks", "stock" + currentTime, jObject);
			Hbase.addData(symbol, currentTime, jObject.toString());
			//System.out.println(symbol+" done!");
			// Hbase.addData(symbol, keyValues);
		} catch (Exception e) {
			if (handleError) {
				// System.out.println(url);
				errors.add(symbol);
				// e.printStackTrace();
			} else {
				// read the symbol in the second time and if false, output the
				// wrong result.
				System.out.println(symbol+" error!");
				//e.printStackTrace();
			}
			// readDetail(symbol);
		}
	}

	/**
	 * Read symbols to symbolList
	 * 
	 * @throws IOException
	 */
	public static void readListFromFile() throws IOException {
		File httpHeaders = new File("Symbols");
		if (httpHeaders.exists()) {
			BufferedReader in = new BufferedReader(new FileReader(httpHeaders));
			String line = in.readLine();
			while (line != null) {
				symbolList.add(line);
				line = in.readLine();
			}
			in.close();
			currentPosition = symbolList.size();
		}
	}

	/**
	 * Get stock list from xueqiu US stock
	 * http://xueqiu.com/stock/cata/stocklist
	 * .json?page=10&size=30&order=desc&orderby
	 * =percent&type=0%2C1%2C2%2C3&_=1435633101 Shanghai stock
	 * http://xueqiu.com/
	 * stock/cata/stocklist.json?page=1&size=60&order=desc&orderby
	 * =percent&type=11%2C12&_=1435648118815 Hong kong stock
	 * http://xueqiu.com/stock
	 * /cata/stocklist.json?page=1&size=90&order=desc&orderby
	 * =percent&type=30&_=1435648422759
	 * 
	 * @throws IOException
	 */
	public static void getStockLists() throws IOException {
		String urlPart1 = "http://xueqiu.com/stock/cata/stocklist.json?page=";
		String urlPart2 = "&size=100&order=asc&orderby=code&type=0%2C1%2C2%2C3%2C11%2C12%2C30&_=";
		File outFile = new File("Symbols");
		BufferedWriter out = new BufferedWriter(new FileWriter(outFile, false));
		boolean stopSign = true;
		int positionPage = 0;
		while (stopSign) {
			positionPage++;
			String url = urlPart1 + positionPage + urlPart2
					+ System.currentTimeMillis();
			stopSign = getStocklist(url, out);
		}
		out.flush(); // 把缓存区内容压入文件
		out.close(); // 最后记得关闭文件
	}

	/**
	 * Read one page, if error read again, then write in file
	 * 
	 * @param url
	 * @return
	 */
	public static Boolean getStocklist(String url, BufferedWriter out) {
		try {
			Connection con = getConnection(url, "GroupHttp");
			Response rs = con.execute();
			JSONObject jObject = new JSONObject(rs.body());
			JSONArray jArray = jObject.getJSONArray("stocks");
			if (jArray.length() == 0) {
				return false;
			} else {
				for (int i = 0; i < jArray.length(); i++) {
					out.write(jArray.getJSONObject(i).get("symbol") + "\r\n");
				}
			}
		} catch (Exception e) {
			System.out.println(url);
			return getStocklist(url, out);
		}
		return true;
	}

	/**
	 * Get stock types from Sina
	 */
	public static void GetSinaStocksType() {
		String url = "http://vip.stock.finance.sina.com.cn/usstock/ustotal.php";
		Connection con = Jsoup.connect(url);
		con.header("User-Agent",
				"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");// 配置模拟浏览器
		Response rs;
		try {
			rs = con.execute();
			if (rs.statusCode() == 200) {
				Document domTree = Jsoup.parse(rs.body(), "gb2312");// 转换为Dom树
				Elements StockList = domTree.getElementsByClass("col_div");
				for (Element stockDiv : StockList) {
					Elements StockListAS = stockDiv.getElementsByTag("a");
					System.out.println("\r\n\r\n\r\n");
					for (Element stockA : StockListAS) {
						String stock = stockA.text();
						stock = stock.substring(stock.indexOf("(") + 1,
								stock.indexOf(")"));
						System.out.println(stock);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// /**
	// * Get a stock's detail information
	// *
	// * @param url
	// * http://xueqiu.com/S/" + symbol
	// * @return Map of type and values
	// */
	// public static Map<String, String> getDetailsBySymbol(String symbol) {
	// String url = "http://xueqiu.com/S/" + symbol;
	// Connection con = Jsoup.connect(url);
	// con.header("User-Agent",
	// "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0");//
	// 配置模拟浏览器
	// Response rs;
	// try {
	// rs = con.execute();
	// if (rs.statusCode() == 200) {
	// Map<String, String> stock = new HashMap<String, String>();
	// Document domTree = Jsoup.parse(rs.body());// 转换为Dom树
	// Element stockTitle = domTree.getElementsByClass("stockTitle")
	// .get(0);
	// Element stockInfo = domTree.getElementsByClass("stockQuote")
	// .get(0);
	// String stockTitleString = stockTitle.getElementsByTag("strong")
	// .get(0).text();
	// // String stockName=stockTitleString.substring(0, endIndex)
	// }
	// } catch (Exception e) {
	// System.out.println(url);
	// e.printStackTrace();
	// // TODO: handle exception
	// }
	// return null;
	// }

	/**
	 * Start multiple threads
	 * 
	 * @param size
	 *            of threads
	 */
	public static void startThreads(int size) {
		queryAllStocks[] threads = new queryAllStocks[size];
		for (int i = 0; i < size; i++) {
			threads[i] = new queryAllStocks();
			threads[i].start();
		}
		for (int i = 0; i < size; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		handleErrors();
		writeLog();

	}

	/**
	 * Write log
	 */
	public static void writeLog() {
		File LogFile = new File("AllStocks");
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(LogFile,
					true));
			out.write(currentTime + "\r\n");
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
