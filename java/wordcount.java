import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	String str = value.toString();				//获取文件，转换为字符串
	String s = "\\d+.\\d+|\\w+";
	Pattern pattern = Pattern.compile(s);
	Matcher ma=pattern.matcher(str);
	while(ma.find()){
		str =ma.group();
 }
	StringTokenizer itr = new StringTokenizer(str);     		//Java StringTokenizer 属于 java.util 包，用于分隔字符串。
	while (itr.hasMoreTokens()) {				// boolean hasMoreTokens()：返回是否还有分隔符。
	word.set(itr.nextToken());				// String nextToken()：返回从当前位置到下一个分隔符的字符串。
	context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      if(sum>=3){
        
        String strin =" ";
        for(int i = 1;i<=sum;i++){
	strin=strin+"+";
           }
      Text rltsum =new Text(new Text(strin));
      result.set(rltsum);
      context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();		//获取配置信息，或者job对象实例
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
   // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}