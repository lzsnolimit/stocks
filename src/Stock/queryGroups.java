package Stock;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class queryGroups extends BaseQuery {

	public static Vector<Map<String, Float>> groups = new Vector<Map<String, Float>>();
	public static String currentTime = new java.text.SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss").format(new Date(System.currentTimeMillis()));
	public static boolean handleError = true;
	public static Vector<String> errors = new Vector<String>();

	static {
		Vector<String> familyNames = new Vector<String>();
		familyNames.add("USAnnualGroup");
		familyNames.add("USMonthlyGroup");
		familyNames.add("USDailyGroup");
		familyNames.add("CNAnnualGroup");
		familyNames.add("CNMonthlyGroup");
		familyNames.add("CNDailyGroup");
		Hbase.createTable(false, familyNames, "StocksGroup");
	}

	/**
	 * Read the symbols of group and write in file
	 */
	public static void readGroupsSymbols(String type) {
		String USAnnualRate = "http://xueqiu.com/cubes/discover/rank/cube/list.json?market=us&sale_flag=0&stock_positions=0&sort=best_benefit&category=12&profit=annualized_gain_rate&page=1&count=150";
		String USMonthlyRate = "http://xueqiu.com/cubes/discover/rank/cube/list.json?market=us&sale_flag=0&stock_positions=0&sort=best_benefit&category=12&profit=monthly_gain&page=1&count=150";
		String USDailyRate = "http://xueqiu.com/cubes/discover/rank/cube/list.json?market=us&sale_flag=0&stock_positions=0&sort=best_benefit&category=12&profit=daily_gain&page=1&count=80";
		String CNAnnualRate = "http://xueqiu.com/cubes/discover/rank/cube/list.json?market=cn&sale_flag=0&stock_positions=0&sort=best_benefit&category=12&profit=annualized_gain_rate&page=1&count=150";
		String CNMonthlyRate = "http://xueqiu.com/cubes/discover/rank/cube/list.json?market=cn&sale_flag=0&stock_positions=0&sort=best_benefit&category=12&profit=monthly_gain&page=1&count=150";
		String CNDailyRate = "http://xueqiu.com/cubes/discover/rank/cube/list.json?market=cn&sale_flag=0&stock_positions=0&sort=best_benefit&category=12&profit=daily_gain&page=1&count=80";

		switch (type) {
		case "US":
			// writeInFile(UsAnnualRate, "UsAnnualGroup");
			// writeInFile(UsMonthlyRate, "UsMonthlyGroup");
			writeInFile(USDailyRate, "USDailyGroup");
			break;
		case "CN":
			// writeInFile(CnAnnualRate, "CnAnnualGroup");
			// writeInFile(CnMonthlyRate, "CnMonthlyGroup");
			writeInFile(CNDailyRate, "CNDailyGroup");
			break;
		default:
			break;
		}

	}

	/**
	 * Write symbols in files
	 * 
	 * @param url
	 * @param fileName
	 */
	public static void writeInFile(String url, String fileName) {
		Response rs = null;
		try {
			Connection con = getConnection(url, "GroupHttp");
			rs = con.execute();
			JSONObject jObject = new JSONObject(rs.body());
			File outFile = new File(fileName);
			BufferedWriter out = new BufferedWriter(new FileWriter("groups/"
					+ outFile, false));
			JSONArray groupsArray = jObject.getJSONArray("list");
			for (int i = 0; i < groupsArray.length(); i++) {
				out.write(groupsArray.getJSONObject(i).getString("symbol")
						+ "\r\n");
			}
			out.flush(); // 把缓存区内容压入文件
			out.close(); // 最后记得关闭文件
		} catch (Exception e) {
			System.out.println(url);
			e.printStackTrace();
		}
	}

	/**
	 * Read symbols from files
	 */

	public static void readSymbolsFromFiles(String type) {
		List<String> symbols;
		try {
			// symbols = readSymbolsFromFile("AnnualGroup");
			// Hbase.setStrings("StocksGroup", "AnnualGroup");
			// groups.clear();
			// for (String symbol : symbols) {
			// parsePage(symbol);
			// }
			// HandleErrors();
			// writeGroups(analyse(), "AnnualGroup");
			//
			// // writeGroups();
			// // analyse(groups, "AnnualGroup");
			//
			// symbols = readSymbolsFromFile("MonthlyGroup");
			// Hbase.setStrings("StocksGroup", "MonthlyGroup");
			// groups.clear();
			// for (String symbol : symbols) {
			// parsePage(symbol);
			// }
			// HandleErrors();
			// writeGroups(analyse(), "MonthlyGroup");
			// writeGroups();
			// analyse(groups, "MonthlyGroup");

			symbols = readSymbols("groups/"+ type + "DailyGroup");
			Hbase.setStrings("StocksGroup", type + "DailyGroup");
			groups.clear();
			for (String symbol : symbols) {
				parsePage(symbol);
			}
			HandleErrors();
			writeGroups(analyse(), type + "DailyGroup");
			updateConfig(type);
			// writeGroups();
			// analyse(groups, "DailyGroup");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
//
//	/**
//	 * Read symbols from file
//	 *
//	 * @param fileName
//	 * @return
//	 * @throws IOException
//	 */
//
//	public static List<String> readSymbolsFromFile(String fileName)
//			throws IOException {
//		List<String> symbols = new ArrayList<String>();
//		File GroupFile = new File("groups/" + fileName);
//		if (GroupFile.exists()) {
//			BufferedReader in = new BufferedReader(new FileReader(GroupFile));
//			String line = in.readLine();
//			while (line != null) {
//				symbols.add(line);
//				line = in.readLine();
//			}
//			in.close();
//		} else {
//			System.out.println("wrong");
//		}
//		return symbols;
//	}

	/**
	 * Parse a page
	 * 
	 * @param symbol
	 */
	public static void parsePage(String symbol) {
		String url = "http://xueqiu.com/P/" + symbol;
		Response rs = null;
		try {
			Map<String, Float> percentage = new HashMap<String, Float>();
			Connection con = getConnection(url, "GroupHttp");
			rs = con.execute();
			Document body = Jsoup.parse(rs.body());
			// System.out.println(body.toString());
			Element weightList = body.getElementsByClass("weight-list").get(0);
			Elements segments = weightList.getElementsByClass("segment");
			Element currentStock = weightList.getElementsByClass("stock")
					.get(0);
			for (int i = 0; i < (segments.size() - 1); i++) {
				String type = segments.get(i).attr("data-segment-name");
				while (currentStock.tag().toString().equals("a")) {
					String currentName = currentStock
							.getElementsByClass("name").get(0).text()
							+ "   "
							+ currentStock.getElementsByClass("price").get(0)
									.text();
					String currentPercentage = currentStock
							.getElementsByClass("stock-weight").get(0).text();
					String currentSymbol = currentStock.attr("href");
					currentSymbol = currentSymbol.substring(currentSymbol
							.lastIndexOf("/") + 1);
					percentage.put(type + "." + currentName + "."
							+ currentSymbol, NumberFormat.getPercentInstance()
							.parse(currentPercentage).floatValue());
					currentStock = currentStock.nextElementSibling();
				}
				currentStock = currentStock.nextElementSibling();
			}
			currentStock = segments.get(segments.size() - 1);
			String currentSymbol = currentStock.attr("data-segment-name");
			String currentPercentage = currentStock
					.getElementsByClass("segment-weight").get(0).text();
			percentage.put(currentSymbol, NumberFormat.getPercentInstance()
					.parse(currentPercentage).floatValue());
			groups.add(percentage);
		} catch (Exception e) {
			if (handleError) {
				errors.add(symbol);
			} else {
				System.out.println(symbol);
				e.printStackTrace();
			}
		}
	}

	// public static void readHbase() {
	// Map<String, Float> AnnualGroups;
	// Map<String, Float> MonthlyGroups;
	// Map<String, Float> DailyGroups;
	// Hbase.setStrings("StocksGroup", "AnnualGroup");
	// ResultScanner results=Hbase.getAllData();
	//
	// }

	/**
	 * Analyse data
	 */
	public static List<Map.Entry<String, Float>> analyse() {
		Map<String, Float> groupsRates = new HashMap<String, Float>();
		List<Map.Entry<String, Float>> list = new ArrayList<Map.Entry<String, Float>>();
		for (Map<String, Float> group : groups) {
			for (Entry<String, Float> pair : group.entrySet()) {
				Float iniValue = (float) 0;
				String key = pair.getKey();
				if (groupsRates.containsKey(key)) {
					iniValue = groupsRates.get(key);
				}
				groupsRates.put(key,
						iniValue + pair.getValue() * 1000 / group.size());
			}
		}
		list.addAll(groupsRates.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Float>>() {
			public int compare(Entry<String, Float> o1, Entry<String, Float> o2) {
				// TODO Auto-generated method stub
				if (o1.getValue() > o2.getValue()) {
					return -1;
				} else if (o1.getValue() == o2.getValue()) {
					return 0;
				} else {
					return 1;
				}
			}
		});

		return list;
	}

	/**
	 * Write data in hbase
	 */
	public static void writeGroups(List<Map.Entry<String, Float>> groupsRates,
			String fileName) {

		File outFile = new File("groups/data/" + currentTime + fileName);
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(outFile,
					false));
			for (Entry<String, Float> entry : groupsRates) {
				if (entry.getKey().contains(".")) {
					String[] keys = entry.getKey().split("\\.");
					out.write(keys[1] + "   " + keys[0] + "   "
							+ entry.getValue().toString() + "\r\n");
					Hbase.addData(keys[1], currentTime, entry.getValue()
							.toString());
				} else {
					Hbase.addData(entry.getKey(), currentTime, entry.getValue()
							.toString());
				}
			}
			out.flush(); // 把缓存区内容压入文件
			out.close(); // 最后记得关闭文件
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void HandleErrors() {
		handleError = false;
		for (String symbol : errors) {
			parsePage(symbol);
		}
		handleError = true;
		errors.clear();
	}

	public static void updateConfig(String type) throws IOException {
		Map<String, String> config = new HashMap<String, String>();
		File GroupFile = new File("groups/" + type + "config");
		if (GroupFile.exists()) {
			BufferedReader in = new BufferedReader(new FileReader(GroupFile));
			String line = in.readLine();
			while (line != null) {
				String[] pairs = line.split("\\=");
				config.put(pairs[0], pairs[1]);
				line = in.readLine();
			}
			in.close();
			config.put("secondNew", config.get("newest"));
			config.put("newest", currentTime);
		} else {
			config.put("secondNew", currentTime);
			config.put("newest", currentTime);
		}

		BufferedWriter out = new BufferedWriter(
				new FileWriter(GroupFile, false));
		for (Entry<String, String> pair : config.entrySet()) {
			out.write(pair.getKey() + "=" + pair.getValue() + "\r\n");
		}
		out.flush();
	}

	public static void query(Boolean updateListAfterQuery, String type) {
		try {
			readSymbolsFromFiles(type);
			AnylyseGroup.start(type, type + "DailyGroup");
			if (updateListAfterQuery) {
				readGroupsSymbols(type);
				readSymbolsFromFiles(type);
			}
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
