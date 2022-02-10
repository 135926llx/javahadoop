package hadoop.avro_test_05.flow;

import hadoop_test.avro_test_05.domain.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean,Text,FlowBean> {

	@Override
	protected void reduce(Text name, Iterable<FlowBean> values,
						  Context context)
			throws IOException, InterruptedException {
		FlowBean tmp=new FlowBean();

		for(FlowBean flowbean:values){
//		flowbean	FlowBean [phobe=13766668888,add=sh,name=ls,consum=9844]
// tmp.add=flowbean.getAdd()

			tmp.setAdd(flowbean.getAdd());
			tmp.setPhone(flowbean.getPhone());
			tmp.setName(flowbean.getName());
//     tmp.getComsum（初始化是0）+flowbean.getConsum()[9844]
//			在第一轮tmp.getConsum()=[9844]

//			第二轮FlowBean [phobe=13766668888,add=sh,name=ls,consum=1000]
//		tmp.Consum	=tmp.getComsum（9844）+flowbean.getConsum()[100]


			tmp.setConsum(tmp.getConsum()+flowbean.getConsum());
		}
		context.write(name, tmp);
	}
}
