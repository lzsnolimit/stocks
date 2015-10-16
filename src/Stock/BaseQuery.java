package Stock;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.Connection.Response;

public class BaseQuery extends Thread {
	/**
	 * Simulate a browser
	 * 
	 * @return Connection
	 * @throws IOException
	 */

	public static Connection getConnection(String url, String fileName)
			throws IOException {
		Connection con = Jsoup.connect(url).ignoreContentType(true);
		File httpHeaders = new File(fileName);
		if (httpHeaders.exists()) {
			BufferedReader in = new BufferedReader(new FileReader(httpHeaders));
			String line = in.readLine();
			while (line != null) {
				if (line.contains("Cookie: ")) {
					String[] cookies = line.substring(line.indexOf(":") + 1)
							.trim().split(";");
					for (String cookie : cookies) {
						con.cookie(cookie.substring(0, cookie.indexOf("="))
								.trim(),
								cookie.substring(cookie.indexOf("=") + 1)
										.trim());
					}
				} else {
					con.header(line.substring(0, line.indexOf(":")).trim(),
							line.substring(line.indexOf(":") + 1).trim());
				}
				line = in.readLine();
			}
			in.close();
		}
		return con;
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

	public static void updateAllSymbols() {
		try {
			getStockLists("CN");
			getStockLists("US");
			getStockLists("HK");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void updateCNSymbols() {
		try {
			getStockLists("CN");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void updateUSSymbols() {
		try {
			getStockLists("US");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void updateHKSymbols() {
		try {
			getStockLists("HK");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	public static void getStockLists(String type) throws IOException {
		String urlPart1 = "http://xueqiu.com/stock/cata/stocklist.json?page=";
		String urlPart2 = null;
		switch (type) {
		case "CN":
			urlPart2 = "&size=100&order=asc&orderby=code&type=11%2C12&_=";
			break;
		case "US":
			urlPart2 = "&size=100&order=asc&orderby=code&type=0%2C1%2C2%2C3&_=";
			break;
		case "HK":
			urlPart2 = "&size=100&order=asc&orderby=code&type=30&_=";
			break;
		default:
			break;
		}

		File outFile = new File(type + "Symbols");
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
	 * Write content in a file
	 * @param fileName
	 * @param content
	 * @param append
	 */
	public static void writeFile(String fileName, String content, boolean append) {
		File outFile = new File(fileName);
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(outFile,
					append));
			out.write(content+"\r\n");
			out.flush(); // 把缓存区内容压入文件
			out.close(); // 最后记得关闭文件
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
