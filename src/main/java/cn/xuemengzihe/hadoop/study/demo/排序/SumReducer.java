package cn.xuemengzihe.hadoop.study.demo.排序;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Text, InfoBean, Text, InfoBean> {

	@Override
	protected void reduce(Text key, Iterable<InfoBean> value, Context context)
			throws IOException, InterruptedException {
		InfoBean info = new InfoBean();
		double in_sum = 0;
		double out_sum = 0;
		for (InfoBean var : value) {
			in_sum += var.getIncome();
			out_sum += var.getExpenses();
		}
		info.setAccount(key.toString());
		info.setIncome(in_sum);
		info.setExpenses(out_sum);
		info.setSurplus(in_sum - out_sum);
		context.write(key, info);
	}
}
