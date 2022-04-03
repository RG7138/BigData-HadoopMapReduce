import com.google.common.collect.Iterables;
import jdk.internal.platform.cgroupv1.SubSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.awt.print.PrinterGraphics;
import java.io.IOException;
import java.util.Iterator;

public class MapReduceHW1 {

    public static class Map extends Mapper <LongWritable,Text,Text,Text>{

        private Text mapkey  = new Text();
        private Text mapValue = new Text();
        private int lineno = 0;

        protected void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException {

            if(value.toString()!=null) {

                String[] mydata = value.toString().split(",");
                lineno++;
                if (mydata.length > 1) {
                    for(int i=0;i<mydata.length;++i){

                        if(i == 3){
                            String [] address = mydata[i].split(" ");

                            for(String str:address){
                                if(str!=null && str!=" " && str!="\t" && str!="\\s+") {
                                    mapkey.set(new Text(str));
                                    mapValue.set(new Text(Integer.toString(lineno)));
                                    context.write(mapkey, mapValue);
                                }
                            }

                        }
                        else{
                            if(mydata[i]!=null && mydata[i]!=" " && mydata[i]!="\t" && mydata[i]!="\\s+") {
                                mapkey.set(new Text(mydata[i]));
                                mapValue.set(new Text(Integer.toString(lineno)));
                                context.write(mapkey, mapValue);
                            }
                        }

                    }

                }
            }

        }

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {

            super.setup(context);
            lineno = 0;

        }

    }

    public static class Reduce extends Reducer<Text,Text,Text,Text> {

        private Text reduceValue = new Text();
        public void reduce(Text key,Iterable<Text> values, Context context) throws IOException,InterruptedException{

            String linenolist = "";
            Iterator<Text> iter = values.iterator();

            while(iter.hasNext()){
                //System.out.println(value.toString());

                String value = iter.next().toString();

                linenolist += value +",";


            }

            linenolist = linenolist.substring(0,linenolist.length()-1);
            linenolist =  "[" + linenolist + "]";

            //System.out.println("Key:"+ key.toString() + "    Value:"+linenolist);

            context.write(key,new Text(linenolist));

        }




    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();

        System.out.println(otherArgs.length);

        if(otherArgs.length != 3 ){

            System.err.println("Usage : Homework1 <input> <output>");
            System.exit(2);

        }

        Job job = new Job(conf, "Homework1");

        job.setJarByClass(MapReduceHW1.class);
        job.setMapperClass(Map.class);

        job.setReducerClass(Reduce.class);

        //set output key type

        job.setOutputKeyClass(Text.class);
        //set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        //set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}