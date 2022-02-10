package hadoop.avro_test_05.flow;

import hadoop.avro_test_05.domain.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		//#实例化
		FlowBean flowBean=new FlowBean();
		//给一个实例的属性赋予初始值，
		flowBean.setPhone(line.split(" ")[0]);
		flowBean.setAdd(line.split(" ")[1]);
		flowBean.setName(line.split(" ")[2]);
		flowBean.setConsum(Integer.parseInt(line.split(" ")[3]));
		System.out.println(flowBean);
		context.write(new Text(flowBean.getName()), flowBean);
	}
}
