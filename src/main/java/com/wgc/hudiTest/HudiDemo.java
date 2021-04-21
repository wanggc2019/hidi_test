package com.wgc.hudiTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import static org.apache.hudi.DataSourceWriteOptions.TABLE_NAME_OPT_KEY;
import static org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs;
import static org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME;


/**
 * HUDI 普通表测试
 * @author lff
 *
 */


public class HudiDemo {

    public static void main(String[] args) throws InterruptedException{

        SparkConf conf = new SparkConf().setAppName("hudiPartitionDemo");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        final String tableName = "hudi_student_mor";
        final String basePath = "/tmp/hudi_student_mor";

        //首先创建输入DStream
        //接收数据流
        //socketTextStream两个参数，第一个监听哪个主机，第二个监听哪个端口
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("134.64.14.230", 6666);

        //开始对接收到的数据执行计算，使用Spark Core提供的算子

        JavaDStream<Student> student = lines.map(new Function<String, Student>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Student call(String line) throws Exception {
                // TODO Auto-generated method stub
                String[] lineSplit = line.split(",");
                Student stu = new Student();
                stu.setUuid(lineSplit[0]);
                stu.setTs(Double.valueOf(lineSplit[1]));
                stu.setId(Integer.valueOf(lineSplit[2]));
                stu.setName(lineSplit[3]);
                stu.setAge(Integer.valueOf(lineSplit[4]));
                return stu;
            }
        });

        student.foreachRDD(new VoidFunction<JavaRDD<Student>>() {
            @Override
            public void call(JavaRDD<Student> studentJavaRDD) throws Exception {
                SparkSession ss = SparkSession.builder()
                        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                        .config("spark.io.compression.codec", "snappy")
                        .config("spark.sql.hive.convertMetastoreParquet", "false")
                        .getOrCreate();
                Dataset studentDF = ss.createDataFrame(studentJavaRDD, Student.class);
                studentDF.write().format("org.apache.hudi").
                        options(getQuickstartWriteConfigs()).
                        option(TABLE_NAME, tableName).
                        mode(SaveMode.Append).
                        save(basePath);
                studentDF.createOrReplaceTempView("students");
                Dataset teenagerDF = ss.sql("select * from students where age >= 23");
                teenagerDF.show();
            }
        });

        //并休眠5秒钟，以便于测试和观察
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }



        //首先对JavaStreamingContext进行一下后续处理
        //必须调用JavaStreamingContext中的start()方法
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

}




