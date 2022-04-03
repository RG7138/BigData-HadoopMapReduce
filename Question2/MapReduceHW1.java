import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import sun.awt.X11.XSystemTrayPeer;

import java.awt.print.PrinterGraphics;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

public class MapReduceHW1 {

    public static class Map extends Mapper <LongWritable,Text,Text,Text>{

        private Text mapkey  = new Text();
        private Text mapValue = new Text();
        private HashMap<String,String> map = new HashMap<String,String>();

        protected void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException {

            String [] mydata = value.toString().split("\t");

            String fr1 = context.getConfiguration().get("friend1");

            //System.out.println(fr1);
            String fr2 = context.getConfiguration().get("friend2");

            String comparekey = (Integer.parseInt(fr1) < Integer.parseInt(fr2))?fr1+","+fr2:fr2+","+fr1;
            //System.out.println(comparekey);
            String friendwithdob = "";

            if(mydata.length >1) {

                String [] frlst = mydata[1].split(",");
                if(frlst.length > 1) {
                    for (String str : frlst) {

                        String userid = (Integer.parseInt(mydata[0]) < Integer.parseInt(str)) ? mydata[0] + "," + str : str + "," + mydata[0];
                        //System.out.println(userid);
                        if (comparekey.equalsIgnoreCase(userid)) {
                            System.out.println("found!!!");
                            for (String tmp : frlst) {

                                String age = map.get(tmp);

                                friendwithdob += tmp + ":" + age + ",";

                            }

                            String finalval = friendwithdob.substring(0, friendwithdob.length() - 1);

                            mapkey.set(new Text(comparekey));
                            mapValue.set(new Text(friendwithdob));
                            context.write(mapkey, mapValue);

                            break;
                        }

                    }
                }
            }
        }

        @Override
        protected void setup(Context context) throws IOException,InterruptedException{

            super.setup(context);

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
                        map.put(str[0], str[9]);
                    }
                    line = br.readLine();

                }

            }
            System.out.println("setup job done!!!!");
            System.out.println(map.size());
        }

    }

    public static class Reduce extends Reducer<Text,Text,Text,Text> {

        //private Text reduceKey = new Text();

        public String checklist(String list1,String list2){

            if(list1 == null || list2 == null){

                return null;
            }
            //System.out.println(list1);
            String [] l1 = list1.split(",");
            String [] l2 = list2.split(",");
            String finallist = "";
            for(String s1:l1){

                for(String s2:l2){

                    if(s1 .equalsIgnoreCase(s2)){
                        finallist += s1 + ",";
                        continue;
                    }

                }

            }

            return finallist;

        }

        public void reduce(Text key,Iterable<Text> values, Context context) throws IOException,InterruptedException{

            System.out.println("mapper side done!!!");
            String[] frlist = new String[2];
            frlist[0] = "";
            frlist[1] = "";
            int index=0;
            //System.out.println("key size"+Iterables.size(values));

            //System.out.println("Here1");

            Iterator<Text> iter = values.iterator();

            while(iter.hasNext() && index<2){
                //System.out.println(value.toString());

                String value = iter.next().toString();
                //System.out.println(value);
                frlist[index] = value;
                index++;
            }


            context.write(key,new Text(frlist[1]));
//            String mutuallist = "";
//            if(frlist[0]!="" && frlist[1]!="") {
//                mutuallist = checklist(frlist[0], frlist[1]);
//            }

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



    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();

        System.out.println(otherArgs.length);

        if(otherArgs.length != 6 ){

            System.err.println("Usage : Homework1 <input> <userdata file> <friend1> <friend2> <output>");
            System.exit(2);

        }

        conf.set("userdata",otherArgs[2]);
        conf.set("friend1",otherArgs[3]);
        conf.set("friend2",otherArgs[4]);
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
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[5]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
