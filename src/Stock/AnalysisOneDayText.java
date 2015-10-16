package Stock;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

public class AnalysisOneDayText {
	private static List<JSONArray> jsonStr;
	private static JSONObject results = new JSONObject();
	private static int currentSize = 7;
	private static Double baseRate = 0.04;

	public static void readStrs() {
		jsonStr = new ArrayList<JSONArray>();
		File symbolFile = new File("results/onedayperiod/highclose/0.04");
		try {
			BufferedReader in = new BufferedReader(new FileReader(symbolFile));
			String line = in.readLine();
			while (line != null) {
				JSONArray temArray = new JSONArray(line);
				if (temArray.length() >= (currentSize + 2)) {
					jsonStr.add(temArray);
				}
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
		System.out.println(jsonStr.size());
	}

	public static Double parseOnePeriod(JSONArray preiod) {
		Double result = 0.0;
		Double total = 0.0;
		for (int i = 0; i < currentSize; i++) {
			Double rate = preiod.getJSONObject(i + 1).getDouble("high")
					/ preiod.getJSONObject(i).getDouble("close") - 1;
			total += rate;
		}
		return total / new Double(currentSize);
	}

	public static void parseAllJsons() {

		List<JSONArray> json4 = new ArrayList<JSONArray>();
		List<JSONArray> json5 = new ArrayList<JSONArray>();
		List<JSONArray> json6 = new ArrayList<JSONArray>();
		List<JSONArray> json7 = new ArrayList<JSONArray>();
		List<JSONArray> json8 = new ArrayList<JSONArray>();
		List<JSONArray> json9 = new ArrayList<JSONArray>();

		for (JSONArray jsonArray : jsonStr) {
			Double everage = parseOnePeriod(jsonArray);
			// System.out.println(everage);
			if (0.04 <= everage && everage < 0.05) {
				json4.add(jsonArray);
			} else if (0.05 <= everage && everage < 0.06) {
				json5.add(jsonArray);
			} else if (0.06 <= everage && everage < 0.07) {
				json6.add(jsonArray);
			} else if (0.07 <= everage && everage < 0.08) {
				json7.add(jsonArray);
			} else if (0.08 <= everage && everage < 0.09) {
				json8.add(jsonArray);
			} else if (0.09 <= everage) {
				json9.add(jsonArray);
			}
		}

		System.out.println(json4.size() + " " + getRate(json4));
		System.out.println(json5.size() + " " + getRate(json5));
		System.out.println(json6.size() + " " + getRate(json6));
		System.out.println(json7.size() + " " + getRate(json7));
		System.out.println(json8.size() + " " + getRate(json8));
		System.out.println(json9.size() + " " + getRate(json9));
	}

	public static Double getRate(List<JSONArray> json) {
		Double size = 0.0;
		for (JSONArray jsonArray : json) {
			// System.out.println(jsonArray.length());
			if (jsonArray.length() > (currentSize + 2)) {
				size++;
			}
		}
		return size / new Double(json.size());
	}
}
