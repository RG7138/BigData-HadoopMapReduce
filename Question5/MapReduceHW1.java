import org.apache.commons.collections.MapIterator;
import org.apache.hadoop.conf.Configuration;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class MapReduceHW1 {

    public static class Map extends Mapper <LongWritable,Text,Text,Text>{

        private Text mapkey  = new Text();
        private Text mapValue = new Text();

        protected void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException {

            if(value.toString()!=null) {

                String[]  mydata = value.toString().split("\t");

                if(mydata.length>1){
                    
                    if(mydata[0]!=null && mydata[0]!="\t") {
                        mapkey.set(new Text(mydata[0]));
                        mapValue.set(new Text(mydata[1]));
                        context.write(mapkey, mapValue);
                    }
                }
            }

        }

    }

    public static class Combine extends Reducer<Text,Text,Text,Text>{

        //private final static IntWritable one  = new IntWritable(1);


        public void reduce (Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{

            Iterator<Text> iter = values.iterator();

            int count=0;
            while(iter.hasNext()){

                String value = iter.next().toString();

                String [] str = value.split(",");

                for(String s:str){
                    count++;
                }


            }

            context.write(new Text("1"),new Text(Integer.toString(count)+"-"+key.toString()));
            //System.out.println("Key:" + key.toString() + " Value:"+ count);
        }

    }

    public static class Reduce extends Reducer<Text,Text,Text,Text> {

        private Text reduceValue = new Text();

        SortedMap<Integer, List<String>> map = new TreeMap<Integer,List<String>>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            System.out.println("Reducer Called!!");
            Iterator<Text> iter = values.iterator();
            Integer val = 0;

            while (iter.hasNext()) {

               String value = iter.next().toString();
               String[] str = value.split("-");
               if(str.length>1){

                   if(map.containsKey(Integer.parseInt(str[0]))){
                       List<String> tmp = map.get(Integer.parseInt(str[0]));

                       tmp.add(str[1]);
                       map.put(val,tmp);
                   }
                   else{
                       List<String> ls = new ArrayList<String>();
                       ls.add(str[1]);
                       map.put(Integer.parseInt(str[0]),ls);
                   }

               }
            }

            Integer k = map.lastKey();
            List<String> lstr = map.get(k);

            for(String word:lstr){
                System.out.println("Key:"+word+" Value:"+k);
                context.write(new Text(word),new Text(Integer.toString(k)));

            }

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
        //job.setNumReduceTasks(1);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Combine.class);
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