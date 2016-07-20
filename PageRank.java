package org.myorg;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PageRank extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(PageRank.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new PageRank(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job1 = Job.getInstance(getConf(), " PageRank1 ");
		job1.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job1, args[0]);
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Red1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.waitForCompletion(true);

		int i;
		for (i = 1; i < 11; i++) {

			Job job2 = Job.getInstance(getConf(), " PageRank2 ");
			job2.setJarByClass(this.getClass());

			FileInputFormat.addInputPath(job2, new Path(args[i]));
			FileOutputFormat.setOutputPath(job2, new Path(args[i + 1]));

			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Red2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			job2.waitForCompletion(true);
		}

		Job job3 = Job.getInstance(getConf(), " PageRank3 ");
		job3.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job3, args[i]);
		FileOutputFormat.setOutputPath(job3, new Path(args[i + 1]));

		job3.setMapperClass(Map3.class);
		job3.setReducerClass(Red3.class);
		job3.setNumReduceTasks(1);
		job3.setSortComparatorClass(SortDesc.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.waitForCompletion(true);

		return job3.waitForCompletion(true) ? 0 : 1;

	}

	// First mapper
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		private static final Pattern WORD_BOUNDARY = Pattern.compile("\n");

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			try {
				String line = lineText.toString();
				Text currentWord1 = new Text();
				Text currentWord2 = new Text();

				for (String word : WORD_BOUNDARY.split(line)) {
					int count = 0;

					if (word.isEmpty())
						continue;

					String title = null, text = null, text1 = "";

					// To separate the things in the title tag
					Pattern pattern1 = Pattern.compile("<title.*?>(.*?)</title>");
					Matcher matcher1 = pattern1.matcher(word);

					// To separate the things in the text tag
					Pattern pattern2 = Pattern.compile("<text.*?>(.*?)</text>");
					Matcher matcher2 = pattern2.matcher(word);

					// key to map2
					if (matcher1.find())
						title = matcher1.group(1);

					if (matcher2.find())
						text = matcher2.group(1);

					// To separate the things in the [[]]
					Pattern pattern3 = Pattern.compile("\\[\\[(.*?)\\]\\]");
					Matcher matcher3 = pattern3.matcher(text);

					while (matcher3.find() != false) {
						text1 = text1 + "#####" + matcher3.group(1);
						count++;
					}

					// value to map2
					text1 = "0.15" + text1 + "#####" + Integer.toString(count);

					currentWord1 = new Text(title);
					currentWord2 = new Text(text1);
					context.write(currentWord1, currentWord2);
				}
			} catch (Exception ex) {
				return;
			}

		}
	}

	// First reducer
	public static class Red1 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text source, Text destinations, Context context) throws IOException, InterruptedException {
			context.write(source, destinations);
		}
	}

	// Second mapper
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String line = value.toString();
				String[] nodeAndDestinations = value.toString().split("\t");
				String[] urlList = nodeAndDestinations[1].toString().split("#####");
				double curRank = Double.parseDouble(urlList[0]);
				int numberOfUrl = Integer.parseInt(urlList[urlList.length - 1]);
				String urlValue = "";
				String rankValue = "#####" + Double.toString(curRank / numberOfUrl);

				// writing url & ranks
				for (int i = 1; i < urlList.length - 1; i++) {
					urlValue = urlValue + "#####" + urlList[i];
					context.write(new Text(urlList[i]), new Text(rankValue));
				}
				// writing url & list of urls
				context.write(new Text(nodeAndDestinations[0]), new Text("@@@@@" + urlValue));

			} catch (Exception ex) {

			}
		}
	}

	// Second reducer
	public static class Red2 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text source, Iterable<Text> destinations, Context context)
				throws IOException, InterruptedException {
			String node = source.toString();
			String value = "", outValue = "";
			double newRank = 0;
			int numUrls = 0;

			for (Text t : destinations) {
				value = t.toString();

				if (value.substring(0, 5).equals("#####")) {
					String part = value.substring(5, value.length());
					newRank += Double.parseDouble(part);
				}

				if (value.substring(0, 5).equals("@@@@@")) {
					String part = value.substring(5, value.length());
					int count = 0;
					for (int i = 0; i < part.length(); i++) {
						if (part.charAt(i) == '#')
							count++;
					}
					numUrls = count / 5;

					outValue = value.substring(5, value.length()) + "#####" + (Integer.toString(numUrls));
				}

			}
			newRank = newRank * 0.85 + 0.15;

			context.write(source, new Text(Double.toString(newRank) + outValue));
		}
	}

	// Third mapper
	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String line = value.toString();
				String[] nodeAndDestinations = value.toString().split("\t");
				String[] urlList = nodeAndDestinations[1].toString().split("#####");
				double curRank = Double.parseDouble(urlList[0]);
				context.write(new Text(Double.toString(curRank)), new Text(nodeAndDestinations[0]));

			} catch (Exception ex) {

			}
		}
	}

	// Third reducer
	public static class Red3 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text rank, Iterable<Text> source, Context context) throws IOException, InterruptedException {

			for (Text t : source) {
				context.write(t, rank);
			}

		}
	}

	// For sorting the pages according to the ranks
	public static class SortDesc extends WritableComparator {

		public SortDesc() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable p1, WritableComparable p2) {

			Text pgrnk1 = (Text) p1;
			Text pgrnk2 = (Text) p2;
			return -1 * pgrnk1.compareTo(pgrnk2);
		}
	}

}
