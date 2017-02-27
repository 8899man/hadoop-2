package cn.xuemengzihe.hadoop.study.demo.排序;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumSortedReducer extends
		Reducer<InfoBean, NullWritable, Text, InfoBean> {

	@Override
	protected void reduce(InfoBean key, Iterable<NullWritable> value,
			Context context) throws IOException, InterruptedException {
		context.write(new Text(key.getAccount()), key);
	}
}
