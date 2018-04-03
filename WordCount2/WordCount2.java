import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
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


public class WordCount2 {
    public static void main(String [] args) throws Exception
    {
        Configuration conf=new Configuration();
        String[] files=new GenericOptionsParser(conf,args).getRemainingArgs();
        Path input=new Path(files[0]);
        Path output=new Path(files[1]);

       
        Job job=new Job(conf,"word count");
        job.setJarByClass(WordCount2.class);
        job.setMapperClass(MapWordCount.class);
        job.setReducerClass(WordCountReduction.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class MapWordCount extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            String line = nopunct(value.toString());
            String palabra2=new String();
            String palabra=new String();

            StringTokenizer itr = new StringTokenizer(line);

//          Inicializando palabra y palabra2.
            if (itr.hasMoreTokens()) {
                palabra = itr.nextToken();
                palabra2 = palabra;
            }


//          Al incio del while palabra2 es puesta con palabra y concatenada con su palabra siguiente.
            while (itr.hasMoreTokens())
            {
                palabra = palabra2;
                palabra2 = itr.nextToken();;
                palabra = palabra+" "+palabra2;

                Text outputKey = new Text(palabra.toLowerCase());
                IntWritable outputValue = new IntWritable(1);
                con.write(outputKey, outputValue);

//          AL final de cada linea solo tomara la ultima palabra como key
                if (!itr.hasMoreTokens())
                {
                    outputKey.set(palabra2.toLowerCase());
                    con.write(outputKey, outputValue);
                }
            }
        }
        public static String nopunct(String s) {
            Pattern pattern = Pattern.compile("[^0-9 a-z A-Z]");
            Matcher matcher = pattern.matcher(s);
            String number = matcher.replaceAll(" ");
            return number;
        }
    }
    public static class WordCountReduction extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
        {
            int sum = 0;
            for(IntWritable value : values)
            {
                sum += value.get();
            }
            con.write(word, new IntWritable(sum));
        }
    }
}
