package cn.xuemengzihe.hadoop.study.demo.倒排索引;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author 李春
 * @time 2017年2月27日 下午2:19:05
 */
public class 倒排索引 {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer strToken = new StringTokenizer(line);
			FileSplit split = (FileSplit) context.getInputSplit();
			Path path = split.getPath();
			String name = path.getName();
			while (strToken.hasMoreTokens()) {
				context.write(new Text(strToken.nextToken() + "->" + name),
						new Text("1"));
			}
		}

	}

	public static class MyCombiner extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer strToken = new StringTokenizer(key.toString());
			long sum = 0;
			for (Text var : value) {
				var.toString();
				sum++;
			}
			context.write(new Text(strToken.nextToken()), new Text("->" + sum));
		};
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			String result = "";
			for (Text var : value) {
				result += var.toString() + ",";
			}
			context.write(key, new Text(result));
		};
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(倒排索引.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setCombinerClass(MyCombiner.class);

		job.waitForCompletion(true);
	}

}
