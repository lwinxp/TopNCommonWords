import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Comparator;
import java.util.LinkedHashMap;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;


public class TopkCommonWordsFinal {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);

        private Text word = new Text();

        private String inputFileName;

        private String stopFilePath;

        private Set<String> patternsToSkip = new HashSet<String>();

        protected void setup(Mapper.Context context)
                throws IOException,
                InterruptedException {

            try {
                stopFilePath = "./commonwords/input/stopwords.txt";

                BufferedReader stopFileReader = new BufferedReader(new FileReader(stopFilePath));

                String stopword;
                while ((stopword = stopFileReader.readLine()) != null) {
                    patternsToSkip.add(stopword);
                }
            } catch (IOException ioe) {
                System.err.println("Error parsing stopwords '"
                        + stopFilePath + "' : " + StringUtils.stringifyException(ioe));
            }

            if (context.getInputSplit() instanceof FileSplit) {
                this.inputFileName = ((FileSplit) context.getInputSplit()).getPath().toString();
            }
        }


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String valueString = value.toString();

            // valueString = valueString.toLowerCase();

            StringTokenizer itr = new StringTokenizer(valueString);

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());

                if (word.toString().isEmpty() || patternsToSkip.contains(word.toString())) {
                    continue;
                }

                Text fileName = new Text(this.inputFileName);

                context.write(word, fileName);

//                cleanWord=word.toString().replaceAll("[^a-zA-Z]","");
//                if (cleanWord.isEmpty() || patternsToSkip.contains(cleanWord)) {
//                    continue;
//                }
//                Text cleanWordText = new Text(cleanWord);
//
//                context.write(cleanWordText, one);
            }
        }
    }

    public static class Combiner extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key, val);
            }
        }
    }
    public static class IntSumReducer
            extends Reducer<Text,Text,IntWritable,Text> {

        public Map<String , Integer > preSort = new HashMap<String , Integer>();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            int file1sum = 0;
            int file2sum = 0;

            for (Text val : values) {

                if(val.toString().contains("task1-input1.txt")){
                    file1sum += 1;
                }
                if(val.toString().contains("task1-input2.txt")){
                    file2sum += 1;
                }
            }

            preSort.put(key.toString(),Math.min(file1sum, file2sum));

        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> unSortedMap = preSort;

            LinkedHashMap<String, Integer> reverseSortedMap = new LinkedHashMap<>();
            unSortedMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .limit(20)
                    .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));

            Set<String> keySet = reverseSortedMap.keySet();
            for(String key:keySet) {
                context.write(new IntWritable(reverseSortedMap.get(key)), new Text(key));
            }

//            for (Map.Entry<String,Integer> entry : reverseSortedMap.entrySet()){
//                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
//            }
        }
    }

    // main function is run
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Topk Common Words 1");

        job.setJarByClass(TopkCommonWordsFinal.class);

        job.setMapperClass(TokenizerMapper.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(Combiner.class);

        job.setReducerClass(IntSumReducer.class);

//        job.setOutputKeyClass(Text.class);
//        job.setOutputKeyClass(IntWritable.class);

//        job.setOutputValueClass(IntWritable.class);
//        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPaths(job, "./commonwords/input/task1-input1.txt,./commonwords/input/task1-input2.txt,./commonwords/input/stopwords.txt");

        FileOutputFormat.setOutputPath(job, new Path("./commonwords/cm_output/"));
        // FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
