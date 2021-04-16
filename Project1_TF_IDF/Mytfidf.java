import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.FileStatus;

public class Mytfidf{

        // document,content -> word@document,1 

        public static class MyMapper1
                extends Mapper<LongWritable, Text, Text, IntWritable>{
                private final static IntWritable one = new IntWritable(1);

                private Text word = new Text();

                public void map(LongWritable key, Text value, Context context)
                        throws IOException, InterruptedException{
                        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

                        String line = value.toString();
                        String match = "[^\uAC00-\uD7A3xfea-zA-Z0-9\\s]";
                        line  = line.replaceAll(match,"");
                        StringTokenizer st = new StringTokenizer(line);

                        while(st.hasMoreTokens()){
                                word.set(st.nextToken().toLowerCase() + "@" + fileName);
                                context.write(word,one);
                          }
                 }
        }
        // word@document,1 -> word@document,n 

        public static class MyReducer1
                extends Reducer<Text, IntWritable, Text, IntWritable>{
                private IntWritable sumWritable = new IntWritable();


                protected void reduce(Text key, Iterable<IntWritable> values,
                        Context context)
                        throws IOException, InterruptedException{

                	int sum =0;
                	for(IntWritable val: values){
                        	sum += val.get();
               	 }

                	sumWritable.set(sum);

                	context.write(key, sumWritable);
                }
        }


        //word@document,n -> word, document = n

         public static class MyMapper2
                extends Mapper<LongWritable, Text, Text, Text>{
                public void map(LongWritable key, Text value, Context context)
                        throws IOException, InterruptedException{

                	String[] count = value.toString().split("\t");
                	String[] wordDoc = count[0].split("@");
                	context.write(new Text(wordDoc[0]), new Text(wordDoc[1] + "=" + count[1]));

                }
        }

        //word,document = n -> word@document , TF-IDF 

        public static class MyReducer2
                extends Reducer<Text, Text, Text, Text>{
                
		protected void reduce(Text key, Iterable<Text> values,
                        Context context)
                        throws IOException, InterruptedException{

                        int D = Integer.parseInt(context.getJobName());
                        int d = 0;
                        Map<String,Integer> tmp = new HashMap<String,Integer>();
                        for(Text val: values){
                                String[] docFre = val.toString().split("=");
                                d++;
                                String term = docFre[0];
                                Integer n = Integer.parseInt(docFre[1]);
                                tmp.put(term,n);
                        }

                        for(String doc : tmp.keySet()){
                       		double tf = tmp.get(doc); 
                        	double idf = Math.log10((double)D / (double)d);
				
                        	double tf_idf = tf*idf;

                        	context.write(new Text(key+ "@" +doc), new Text(Double.toString(tf_idf)));
                        }

                }
        }

        public static void main(String[] args) throws Exception{
                Configuration config = new Configuration();

		Path input = new Path(args[0]);
                Path path = new Path("/temp/fre");

                Job job1 = Job.getInstance(config,"tf");
                job1.setJarByClass(Mytfidf.class);
                job1.setMapperClass(MyMapper1.class);
                job1.setReducerClass(MyReducer1.class);
                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(IntWritable.class);
                job1.setInputFormatClass(TextInputFormat.class);
                job1.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.addInputPath(job1, input);
                FileOutputFormat.setOutputPath(job1, path);

                job1.waitForCompletion(true);

                Job job2 = Job.getInstance(config, "idf");
                job2.setJarByClass(Mytfidf.class);
                job2.setMapperClass(MyMapper2.class);
                job2.setReducerClass(MyReducer2.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);
                job2.setInputFormatClass(TextInputFormat.class);
                job2.setOutputFormatClass(TextOutputFormat.class);


                FileInputFormat.addInputPath(job2, path);
                FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		FileSystem fs = input.getFileSystem(config);
		FileStatus[] st = fs.listStatus(input);
		job2.setJobName(String.valueOf(st.length));

                job2.waitForCompletion(true);

        }

}
