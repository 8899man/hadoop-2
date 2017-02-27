package cn.xuemengzihe.hadoop.study.demo.排序;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class 排序 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(排序.class);

		job.setMapperClass(SumMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoBean.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if (job.waitForCompletion(true))
			System.out.println("执行成功！");
	}
}
