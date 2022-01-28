package com.LiShiJi.MapReduce;


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


/**
 * 单词统计
 */

/**
 * hadoop,java,python,zookeeper
 * kafka,hadoop,java,python,zookeeper
 * kafka
 * kafka,hadoop,java,python,zookeeper
 * kafka,hadoop,java,python,zookeeper
 * kafka,hadoop,java,python,zookeeper
 */
public class WordCount {

    //Map

    /**
     * Mapper<LongWritable, Text,Text,LongWritable>
     * Mapper<输入Map的key的类型, 输出Map的Value的类型,Map输出key的类型,Map输出Value的类型>
     * Map默认的inputformat是什么？    TextInputformat
     * TextInputformat: 会将数据每一行的偏移量作为key,每一行作为value输入到Map端
     */
    public static class MyMapper extends Mapper<LongWritable, Text,Text,LongWritable>{
        //实现自己的逻辑

        /**
         *
         * @param key  输入map的key
         * @param value 输入map的value
         * @param context MapReduce程序的上下文环境，Map端的输出可以通过context发送到Reduce端
         * @throws IOException
         * @throws InterruptedException
         */

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //要输入的是自己需要实现的逻辑
            //获取第一行数据
            //hadoop,java,python,zookeeper
            String line = value.toString();
            //按逗号分割
            String[] split = line.split(",");
            for (String word : split) {
                //将结果发送到reduce端
                context.write(new Text(word),new LongWritable(1));   //hadoop ---> 1
            }

        }
    }



    //reduce

    /**
     * Reducer<Text,LongWritable,Text,LongWritable>
     * Reducer<Map输出key的类型,Map输出value的类型,Reduce输出key的类型,Reduce输出value的类型>
     */
    public static class MyReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
        //实现自己的逻辑

        /**
         *
         * @param key 从Map端输出并经过分组后的key（相当于对Map输出的key做一个group by）
         * @param values 从Map端输出并经过分组后的key  对应的value的集合
         * @param context MapReduce程序的上下文环境，Reduce端的输出可以通过context最终写到HDFS
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                long l = value.get();
                sum += l;
            }

            //最终将结果输出
            context.write(key,new LongWritable(sum));
        }
    }


    //MapReduce任务
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        //配置reduce最终输出的key value的分隔符为  #
        conf.set("mapred.textoutputformat.separator","#");
        //做其它配置
        //创建一个Job
        Job job = Job.getInstance(conf);

        //设置Reduce的个数
        job.setNumReduceTasks(2);

        //设置job的名字
        job.setJobName("WordCountApp");

        //设置MapReduce运行类
        job.setJarByClass(WordCount.class);
        //配置Map
        //配置Map Task 运行类
        job.setMapperClass(MyMapper.class);

        //设置Map任务输出key的类型
        job.setMapOutputKeyClass(Text.class);
        //设置Map任务输出value的类型
        job.setMapOutputValueClass(LongWritable.class);

        //配置Reduce
        //配置Reduce Task 运行的类
        job.setReducerClass(MyReduce.class);

        //设置Reduce任务输出key的类型
        job.setOutputKeyClass(Text.class);

        //设置Reduce任务输出value的类型
        job.setOutputValueClass(LongWritable.class);

        //配置输入输出路径
        //将第一个参数作为输入路径，第二个参数作为输出路径
        FileInputFormat.addInputPath(job,new Path(args[0]));

        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交任务并等待运行结束
        job.waitForCompletion(true);


    }




}
