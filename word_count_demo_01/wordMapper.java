package hadoop.word_count_demo_01;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//mapper进程，每一个split（block）会启动该类，
public class wordMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

    @Override
//    map方法，对一个block里面的数据 按行 进行读取，处理
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//  LongWritable key: 指的是偏移量。
// Text value: 每一行的内容
// Context context：上下文
        //    value  =    he  love  bigData

        String line =  value.toString();

        System.out.println(line);

//        data =[he,love,bigData]
        String[] data = line.split(" ");
        System.out.println(line);
//         String ts=  data[3]    ;
        for (String word:
                data
             ) {
//            if(Integer.parseInt(ts)>30 || Integer.parseInt(ts) <39){
//
//            }
//            new Text(word),new LongWritable(1),,  （chess,1）
            context.write(new Text(word),new LongWritable(1));
        }



    }


}