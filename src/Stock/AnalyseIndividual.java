package Stock;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class AnalyseIndividual {
	private static String TableName = "Stocks";
	public static String currentTime = new java.text.SimpleDateFormat(
			"yyyy-MM-dd").format(new Date(System.currentTimeMillis()));
	/**
	 * Map class
	 * 
	 * @author Zhongshan Lu
	 *
	 */
	public static class MyMapper extends TableMapper<FloatWritable, Text> {

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
	public static void start() throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "StockSummary");
		job.setJarByClass(App.class); // class that contains mapper and reducer
		Scan scan = new Scan();
		Filter ColumnFilter = new QualifierFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(currentTime)));
		scan.setFilter(ColumnFilter);
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs

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
	public static void AnalyseShiyinglv(String jsonStr) {
		
	}

}