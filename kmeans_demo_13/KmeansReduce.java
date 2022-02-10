package hadoop.kmeans_demo_13;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KmeansReduce extends Reducer<Text, Text, Text,Text> {
    public void reduce(Text key, Iterable<Text> value, Context context)
            throws IOException,InterruptedException{
        //key为类编号，value为类内所有样本,Iterable<Text> value，海量数据，类少的情况下 value是海量的
        //combiner可以用到，
        long num=0;
//        顶以一个k维度数，存储累加结果
        double[] re=new double[DataSource.feat_num];

//        遍历value计算累加值，并标记样本个数,
        for(Text T:value){
            num++;
            String onePoint=T.toString();
            onePoint=onePoint.replace(",", " ");
            String[] parameters=onePoint.split(" ");
            //进行累加,此处仅有两个元素，如果多个元素该如何
            for (int i = 1; i <parameters.length ; i++) {
                re[i-1]+=Double.parseDouble(parameters[i]);
            }

        }
        String result=key.toString()+",";
        for (int i = 0; i <re.length ; i++) {
            result=result+re[i]/num+",";
        }

        result.substring(0,result.length() -1);
        context.write(key,new Text(result));
    }
}
