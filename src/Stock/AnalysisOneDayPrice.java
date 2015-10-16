package Stock;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.json.JSONArray;

public class AnalysisOneDayPrice {
	private static String TableName = "StocksPrice";

	/**
	 * Map class
	 * 
	 * @author Zhongshan Lu
	 *
	 */
	public static class MyMapper extends TableMapper<FloatWritable, Text> {
		private String tableName;

		/**
		 * map method
		 */
		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {

			for (Cell cell : value.rawCells()) {
				System.out.println("RowName:"
						+ new String(CellUtil.cloneRow(cell)) + " ");
				System.out.println("value:"
						+ new String(CellUtil.cloneValue(cell)) + " ");

			}

		}

	}

	/**
	 * Reducer class
	 * 
	 * @author Zhongshan lu
	 *
	 */
	public static class MyTableReducer extends
			TableReducer<FloatWritable, Text, ImmutableBytesWritable> {

		/**
		 * reduce method
		 */
		public void reduce(FloatWritable key, Iterable<Text> values,
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
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "StockSummary");
		job.setJarByClass(App.class); // class that contains mapper and reducer
		Scan scan = new Scan();
		// scan.setMaxVersions();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		Filter familyFilter = new FamilyFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(type)));
		scan.setFilter(familyFilter);

		TableMapReduceUtil.initTableMapperJob(TableName, // input table
				scan, // Scan instance to control CF and attribute selection
				MyMapper.class, // mapper class
				FloatWritable.class, // mapper output key
				Text.class, // mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob(TableName, // output
				// table
				MyTableReducer.class, // reducer class
				job);
		job.setNumReduceTasks(1); // at least one, adjust as required

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

	/**
	 * Parse a price
	 * 
	 * @param jsonStr
	 * @param rates
	 * @param volumns
	 */
	public void ParsePrice(String jsonStr, Double period, List<Long> startTime,
			List<Double> rates, List<Double> volumns) {
		JSONArray jsonArrays = new JSONArray(jsonStr);
		for (int i = 0; i < jsonArrays.length(); i++) {
			new java.text.SimpleDateFormat("yyyy-MM-dd").format(new Date(System
					.currentTimeMillis()));
		}
	}

	public static Long ParseTime(String UnixTime) {
		SimpleDateFormat myFmt = new SimpleDateFormat(
				"EEE MMM dd HH:mm:ss z yyyy");
		//TimeZone.setDefault(TimeZone.getTimeZone("America/Newyork"));
		try {
			Date time=myFmt.parse(UnixTime);
			time = changeTimeZone(time, TimeZone.getDefault(), TimeZone.getTimeZone("America/New_York"));  
			System.out.println(time.toString());
			return myFmt.parse(UnixTime).getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}
	
	/** 
	 * 获取更改时区后的日期 
	 * @param date 日期 
	 * @param oldZone 旧时区对象 
	 * @param newZone 新时区对象 
	 * @return 日期 
	 */  
	
	public static Date changeTimeZone(Date date, TimeZone oldZone, TimeZone newZone) {  
	    Date dateTmp = null;  
	    if (date != null) {  
	        int timeOffset = oldZone.getRawOffset() - newZone.getRawOffset(); 
	        dateTmp=new Date(date.getTime() - timeOffset);  
	    }  
	    return dateTmp;  
	}  
}
