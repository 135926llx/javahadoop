package hadoop.friend_11;

import hadoop_test.Utils_hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.Arrays;

public class FindFriend {
    /*
    * 1.遍历文件内容，讲朋友为key，用户为值，输出
    *
    *
    *
    * */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        step1
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(FindFriend.class);
        job1.setMapperClass(FriendMapper.class);
        job1.setReducerClass(FriendReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job1,new Path("/hadoop_test/findFriend/friend.txt"));

        if( Utils_hadoop.testExist(conf,"/hadoop_test/findFriend/result")){
            Utils_hadoop.rmDir(conf,"/hadoop_test/findFriend/result");}
        FileOutputFormat.setOutputPath(job1, new Path("/hadoop_test/findFriend/result"));
        boolean res1=job1.waitForCompletion(true);


//        step1
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(FindFriend.class);
        job2.setMapperClass(FriendMapper1.class);
        job2.setReducerClass(FriendReducer1.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job2,new Path("/hadoop_test/findFriend/result"));
        if( Utils_hadoop.testExist(conf,"/hadoop_test/findFriend/result1")){
            Utils_hadoop.rmDir(conf,"/hadoop_test/findFriend/result1");}
        FileOutputFormat.setOutputPath(job2, new Path("/hadoop_test/findFriend/result1"));
        boolean res2 = job2.waitForCompletion(true);

        System.exit(res1?0:1);


    }


}
class FriendMapper  extends Mapper<LongWritable,Text, Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String uid = line.split("\t")[0];

        String[] friends = line.split("\t")[1].split(",");
//    uid孙诗云
//    friends[钱白晴,李夏彤]
        for (String fre: friends
        ) {

//     钱白晴 孙诗云
//     李夏彤 孙诗云
//     钱白晴 李磊
//     钱白晴 王芳
            context.write(new Text(fre),new Text(uid));
        }


    }
}
class FriendReducer extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//      钱白晴， [孙诗云,李磊,王芳  ]
        StringBuilder sb = new StringBuilder();
        for (Text fre: values
             ) {
            sb.append(fre.toString()).append(",");
        }
        String re=sb.substring(0,sb.length()-1);

        context.write(key,new Text(re));

        /// 孙初丹   刘领会_李四_王五
    }
}
class FriendMapper1 extends Mapper<LongWritable,Text, Text,Text>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        /// 孙初丹   刘领会_李四_王五

        String line = value.toString();

        String[] lines= line.split("\t");
        //拿到朋友
        String uid = lines[0];
        //拿到朋友
        String[] fri = lines[1].split(",");
        //针对朋友一个排序
//        [刘领会，李四,王五]
        //  刘灵会，李四   孙初丹
        Arrays.sort(fri);
        for (int i = 0; i <fri.length-1 ; i++) {
            for (int j = i+1; j <fri.length ; j++) {
                context.write(new Text(fri[i]+"_"+fri[j]),new Text(uid));
            }
        }


    }
}
class FriendReducer1 extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        StringBuilder sb = new StringBuilder();
        for (Text fre:values
             ) {
            sb.append(fre+",");
        }
        String re = sb.substring(0,sb.length()-1);
        context.write(new Text(key),new Text(re));



    }
}
