import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MultipleJobs {

	private static final int TOP_CITIES = 100;
	private static final String LAST_YEAR = "2019";
	private static final String SECOND_LAST_YEAR = "2018";

	/**
     * First Mapper
     */
	public static class FirstMapper extends Mapper<Object, Text, Text, MyValue> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Read and split correctly the CSV file columns
			String[] tokens = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			// Get the needed columns
			String severity = tokens[3];
			String year = tokens[4].substring(0, 4);
			String city = tokens[15];
			// Map kv as (cityYear, (severity, 1)) only for records about the last and second-last year
			if (year.equals(LAST_YEAR) || year.equals(SECOND_LAST_YEAR)) {
				String cityYear = year + "," + city;
				context.write(new Text(cityYear), new MyValue(new Text(severity), new DoubleWritable(1)));
			}
		}
	}


	/**
	 * Combiner
	 */
	public static class Combiner extends Reducer<Text, MyValue, Text, MyValue> {

		public void reduce(Text key, Iterable<MyValue> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			double count = 0;
			for (MyValue val : values) {
				sum += Double.parseDouble(val.getK().toString());
				count += val.getV().get();
			}
			context.write(key, new MyValue(new Text(String.valueOf(sum)), new DoubleWritable(count)));
		}
	}

	/**
	 * First Reducer
	 */
	public static class FirstReducer extends Reducer<Text, MyValue, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<MyValue> values, Context context) throws IOException, InterruptedException {
			double tot = 0;
			double count = 0;
			// For each severity value related to a cityYear key, sum the values
			for (MyValue val : values) {
				tot += Double.parseDouble(val.getK().toString());
				count += val.getV().get();
			}
			// Take for each cityYear the average severity value
			context.write(key, new DoubleWritable(tot/count));
		}
	}
	
	/**
	 * Second Mapper
	 */
	public static class SecondMapper extends Mapper<Object, Text, Text, MyValue> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Split the previous map-reduce output
			String[] kv = value.toString().split("\t");
			// Get the columns and extract the year and the city
			String year = kv[0].substring(0, 4);
			String city = kv[0].substring(5);
			String severityAvg = kv[1];
			// Map kv as (city, (year, severityAvg))
			context.write(new Text(city), new MyValue(new Text(year), new DoubleWritable(Double.parseDouble(severityAvg))));
		}
	}

	/**
	 * Second Reducer
	 */
	public static class SecondReducer extends Reducer<Text, MyValue, Text, Text> {
		// Needed to collect the final value for each city
		private Map<String, Double> map;

		// Method which runs only once at the beginning in the lifetime of the reducer
		public void setup(Context context) {
			map = new HashMap<>();
		}

		public void reduce(Text key, Iterable<MyValue> values, Context context) {
			// Needed to order by years the average severity for each city
			TreeMap<String, Double> tmap = new TreeMap<>();
			// For every (year, severityAvg) value add it to the TreeMap
			for (MyValue val : values) {
				tmap.put(val.getK().toString(), Double.parseDouble(val.getV().toString()));
			}
			// If false means there is not a last or second-last year value related to a city
			if (tmap.size() == 2) {
				double lastYearSeverityAvg = tmap.lastEntry().getValue();
				double secondLastYearSeverityAvg = tmap.firstEntry().getValue();
				// Put inside the map the difference between the last and second-last severityAvg and
				// multiply by -1 for a later ordering logic
				map.put(key.toString(), (-1) * (lastYearSeverityAvg - secondLastYearSeverityAvg));
			}
		}

		// Method which runs only once at the end in the lifetime of the reducer
		public void cleanup(Context context) throws IOException, InterruptedException {
			// Needed to have a maximum of 4 decimals in severity value
			DecimalFormat df = new DecimalFormat("#.####");
			// Ordered entry set of all cities
			List<Map.Entry<String, Double>> entryList = new ArrayList<>(sortByValue(map).entrySet());
			// Take only the top TOP_CITIES key-value
			for (Map.Entry<String, Double> entry : entryList.subList(0, TOP_CITIES)) {
				// Multiply by -1 to get the original value
				context.write(new Text(entry.getKey()), new Text(String.valueOf(df.format(entry.getValue() * (-1)))));
			}
		}
	}

	// Utility method to sort a hashmap by value
	public static Map<String, Double> sortByValue(Map<String, Double> map) {
		// Create a list from elements of HashMap
		List<Map.Entry<String, Double>> list = new LinkedList<>(map.entrySet());
		// Sort the list
		Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
			public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});
		// Put data from sorted list to hashmap
		HashMap<String, Double> temp = new LinkedHashMap<>();
		for (Map.Entry<String, Double> x : list) {
			temp.put(x.getKey(), x.getValue());
		}
		return temp;
	}

	@SuppressWarnings("rawtypes")
	public static class MyValue implements WritableComparable {

		private Text k;
		private DoubleWritable v;

		public MyValue() {
			this.k = new Text("");
			this.v = new DoubleWritable(-1);
		}

		public MyValue(Text k, DoubleWritable v) {
			this.k = k;
			this.v = v;
		}

		public Text getK() {
			return this.k;
		}

		public DoubleWritable getV() {
			return this.v;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.k.readFields(in);
			this.v.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			this.k.write(out);
			this.v.write(out);
		}

		@Override
		public int compareTo(Object o) {
			MyValue other = (MyValue) o;
			return this.v.compareTo(other.getV());
		}
	}

	public static void main(String[] args) throws Exception {
		Job job1 = Job.getInstance(new Configuration(), "First job");
		Job job2 = Job.getInstance(new Configuration(), "Second job");

		// Checking arguments
		String[] otherArgs = new GenericOptionsParser(new Configuration(), args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Error: please provide three paths.");
			System.exit(2);
		} else if (otherArgs.length > 3) {
			if (Integer.parseInt(args[3]) >= 0) {
				job1.setNumReduceTasks(Integer.parseInt(args[3]));
			}
		}

		// Get the paths
		Path inputPath = new Path(args[0]);
		Path output1Path = new Path(args[1]);
		Path output2Path = new Path(args[2]);
		// Check if output paths have already present to delete them
		FileSystem fs = FileSystem.get(new Configuration());
		if (fs.exists(output1Path)) {
			fs.delete(output1Path, true);
		}
		if (fs.exists(output2Path)) {
			fs.delete(output2Path, true);
		}

		// JOB 1
		job1.setJarByClass(MultipleJobs.class);
		job1.setMapperClass(FirstMapper.class);
		job1.setCombinerClass(Combiner.class);
		job1.setReducerClass(FirstReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(MyValue.class);
		FileInputFormat.addInputPath(job1, inputPath);
		FileOutputFormat.setOutputPath(job1, output1Path);
		job1.waitForCompletion(true);

		// JOB 2
		job2.setJarByClass(MultipleJobs.class);
		job2.setMapperClass(SecondMapper.class);
		job2.setReducerClass(SecondReducer.class);
		job2.setNumReduceTasks(1);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(MyValue.class);
		FileInputFormat.addInputPath(job2, output1Path);
		FileOutputFormat.setOutputPath(job2, output2Path);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
