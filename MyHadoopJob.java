import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

public class MyHadoopJob {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
        conf.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
            System.err.println("Usage: Inverted Indices <input> <output>");
            System.exit(-1);
        }
        Job job = Job.getInstance(conf, "Inverted Indices");
        job.setJarByClass(MyHadoopJob.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i=0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("hadoopjob.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

        public Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = value.toString().replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();

            StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, new Text(fileName));
            }

        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
            DefaultHashMap<String, Integer> word_map = new DefaultHashMap<>(0);

            for (Text word : vals) {
                int word_count = word_map.get(word.toString()) + 1;
                word_map.put(word.toString(), word_count);
            }

            for (HashMap.Entry<String, Integer> entry : word_map.entrySet()) {
                context.write(key, new Text(entry.getKey() + "-->" + entry.getValue()));
            }
        }

    }

    static class DefaultHashMap<K, V> extends HashMap<K, V> {
        protected V defaultValue;
        public DefaultHashMap(V defaultValue) {
            this.defaultValue = defaultValue;
        }
        @Override
        public V get(Object k) {
            return containsKey(k) ? super.get(k) : defaultValue;
        }
    }
}