package Stock;

/**
 * Hello Big Data!
 *
 */
public class App {

	public static void main(String[] args) throws Exception {
		 //queryAllStocks.startThreads(10);

		// queryGroups.query(true, "CN");// bipan true
		// queryGroups.query(true, "US");// bipan true

		// QuerySinaFiveDaysPrice.startThreads(10, 5, "US");
		// AnalysisFiveDaysPrice.start("US");

		// QueryStocksHistory.startThreads(10, "3m");
//		QueryOneDayPeriod.startThreads(10, true, "CN");

		AnalyseOneDayPeriod.startWithRateRange(0.04, 0.105, "CN");
		
		//AnalyseOneDayPeriod.start("CN");
//		AnalysisOneDayText.readStrs();
//		AnalysisOneDayText.parseAllJsons();
		//AnalyseIndividual.start();
		//QueryFiveDaysPrice.startThreads(10, "CN", false);
		// AnalyseHistory.start("CN", 12);
		// AnalyseHistory.start("CN", 13);
		// AnalyseHistory.start("CN", 14);
		// Hbase.setStrings("Stock", "All");
		// AnalyseOneDayPeriod.analysisOneDayPeriod("EFOI",
		// Hbase.getData("RJET"));
		// AnalyseOneDayPeriod.analyseResult();
		// AnalyseOneDayPeriod.start("US");

		// System.out.println("US");
		// for (int i = 3; i <= 15; i++) {
		// for (int j = 2; j <= 15; j++) {
		// AnalyseOneDayPeriod.PeriodRang = i;d
		// AnalyseOneDayPeriod.baseRate = new Double(j) / new Double(100);
		// AnalyseOneDayPeriod.start("US");
		// }
		// }
		//
		//
		// System.out.println("CN");
		// for (int i = 5; i <= 15; i++) {
		// for (int j = 2; j <= 10; j++) {
		// AnalyseOneDayPeriod.PeriodRang = i;
		// AnalyseOneDayPeriod.baseRate = new Double(j) / new Double(100);
		// AnalyseOneDayPeriod.start("CN");
		// }
		// }
		//

		// for (int i = 5; i < 30; i++) {
		// for (int j = 5; j < 15; j++) {
		// AnalyseOneDayPeriod.PeriodRang = j;
		// AnalyseOneDayPeriod.baseRate = new Double(i) / new Double(100);
		// AnalyseOneDayPeriod.start("US");
		// }
		// }

		// AnalyseOneDayPeriod.PeriodRang = 5;
		// AnalyseOneDayPeriod.baseRate = new Double(4) / new Double(100);
		// AnalyseOneDayPeriod.start("US");
	}
}
