import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

public class MapReduceHW1 {

    public static class Map extends Mapper <LongWritable,Text,Text,Text>{

        private Text mapkey  = new Text();
        private Text mapValue = new Text();

        protected void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException {

            String [] mydata = value.toString().split("\t");

            if(mydata.length >1) {

                //System.out.println("Key:" + mydata[0] + "Value:" + mydata[1]);

//                String [] frlist = mydata[1].split(",");
//
//                for(String fr : frlist){
//
//                    if(mydata[0].equalsIgnoreCase(fr)){
//                        continue;
//                    }
//
//                    String genkey = (Integer.parseInt(mydata[0]) < (Integer.parseInt(fr)))?mydata[0]+","+fr:fr+","+mydata[0];
//
//                    mapkey.set(genkey);
//                    mapValue.set(mydata[1]);
//                    context.write(mapkey,mapValue);
//                }
                //System.out.println(mydata[0]);
                mapkey.set(mydata[0]);
                mapValue.set(mydata[1]);
                context.write(mapkey,mapValue);
            }
        }

    }

    public static class Reduce extends Reducer<Text,Text,Text,Text> {

//        private Text reduceKey = new Text();
//        private Text reduceValue

        private HashMap<String,String> map = new HashMap<String,String>();



//        public String checklist(String list1,String list2){
//
//            if(list1 == null || list2 == null){
//
//                return null;
//            }
//            //System.out.println(list1);
//            String [] l1 = list1.split(",");
//            String [] l2 = list2.split(",");
//            String finallist = "";
//            for(String s1:l1){
//
//                for(String s2:l2){
//
//                    if(s1 .equalsIgnoreCase(s2)){
//                        finallist += s1 + ",";
//                        continue;
//                    }
//
//                }
//
//            }
//
//            return finallist;
//
//        }

        public void reduce(Text key,Iterable<Text> values, Context context) throws IOException,InterruptedException{

            String[] frlist = new String[2];

            //System.out.println(key.toString()+" "+values.toString());

//            frlist[0] = "";
//            frlist[1] = "";
//            int index=0;
            //System.out.println("key size"+Iterables.size(values));


            //System.out.println("Here1");

            Iterator<Text> iter = values.iterator();

                while(iter.hasNext() ){
                    //System.out.println(value.toString());
                    String value = iter.next().toString();
                    //System.out.println(key.toString()+" "+value.toString());
//                    String value = iter.next().toString();

                    String[] strs = value.split(",");
                    int minage = Integer.MAX_VALUE;
                    for(String st:strs){

                       String age = map.get(st);
                       if(Integer.parseInt(age)<minage){

                           minage = Integer.parseInt(age);

                       }

                    }

                    System.out.println("key:"+key.toString()+"Value:"+minage);

                    context.write(key,new Text(Integer.toString(minage)));
//                    frlist[index] = value;
//                    index++;
                }



//            String mutuallist = "";
//            if(frlist[0]!="" && frlist[1]!="") {
//                mutuallist = checklist(frlist[0], frlist[1]);
//            }
//
//            if(mutuallist!=null && mutuallist.length()!=0){
//
//                mutuallist = mutuallist.substring(0,mutuallist.length()-1);
//                if(key.toString().equalsIgnoreCase("0,1")  || key.toString().equalsIgnoreCase("20,28193") || key.toString().equalsIgnoreCase("1,29826") || key.toString().equalsIgnoreCase("6222,19272")  || key.toString().equalsIgnoreCase("28041,28056") ) {
//                    System.out.println(key + " "+  mutuallist);
//                }
//                context.write(key, new Text(mutuallist));
//            }
//            else{
//                if(key.toString().equalsIgnoreCase("0,1")  || key.toString().equalsIgnoreCase("20,28193") || key.toString().equalsIgnoreCase("1,29826") || key.toString().equalsIgnoreCase("6222,19272")  || key.toString().equalsIgnoreCase("28041,28056")) {
//                    System.out.println(key + " "+  "null");
//                }
//                context.write(key, new Text("null"));
//            }

        }

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {

            super.setup(context);

            LocalDateTime date = LocalDateTime.now();

            DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy");

            String strdate = date.format(format);

            System.out.print(strdate);

            Configuration conf = context.getConfiguration();

            Path userdata_path = new Path(context.getConfiguration().get("userdata"));

            FileSystem fs = FileSystem.get(conf);

            FileStatus[] fss = fs.listStatus(userdata_path);

            for(FileStatus status:fss){

                Path p = status.getPath();

                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));

                String line;

                line = br.readLine();

                while(line!=null){

                    String[] str = line.split(",");
                    //System.out.println(str.length);
                    if(str.length == 10) {

                        String strd = str[9].substring(str[9].length()-4,str[9].length());

                        System.out.println(strd);

                        int age = Math.abs(Integer.parseInt(strdate)- Integer.parseInt(strd));

                        String stage = Integer.toString(age);
                        //System.out.println(stage);
                        map.put(str[0], stage);
                    }
                    line = br.readLine();

                }

            }
            System.out.println("setup job done!!!!");
            System.out.println(map.size());
        }


    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();

        System.out.println(otherArgs.length);

        if(otherArgs.length != 4 ){

            System.err.println("Usage : Homework1 <input> <userdata> <output>");
            System.exit(2);

        }

        conf.set("userdata",otherArgs[2]);

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
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}