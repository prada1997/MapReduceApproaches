

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Task1_1_Stripes {

    public static class StripesMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

            private MapWritable wordOccurenceList = new MapWritable();
            private Text word = new Text();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                //removing the symbols from the text line using replaceAll method
                //splitting the text line using regex.
                String[] tokens = value.toString().trim().replaceAll("[^a-zA-Z0-9\\s]","").split("\\s+");

                //if the text line is greater than one word then
                if (tokens.length > 1) {

                    //iterating over the words present in the text line
                    for (int i = 0; i < tokens.length; i++) {

                        wordOccurenceList.clear();

                        //setting the word in wordpair class as key
                        word.set(tokens[i].trim());

                        //iterating over the words which are neighbour present in the text line
                        for (int j = 0; j < tokens.length; j++) {

                            //if the key word and neighbour is not equal
                            //then setting the word as neighbour for the key word
                            if (i != j){

                                //if the map contains the neighbour then add its count.
                                if(wordOccurenceList.containsKey(new Text(tokens[j].trim()))){
                                    IntWritable count = (IntWritable) wordOccurenceList.get(new Text(tokens[j].trim()));
                                    count.set(count.get()+1);
                                }
                                else
                                    //if the neighbour is not present in the map then add the neighbour and its count
                                    wordOccurenceList.put((new Text(tokens[j].trim())), new IntWritable(1));
                            }
                        }

                        //emitting the word and its neighbour along with occurrence
                        context.write(word, wordOccurenceList);
                    }
                }
            }
    }

    public static class StripesReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
        private MapWritable totalWordCountMap = new MapWritable();

        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

            totalWordCountMap.clear();

            for (MapWritable value : values) {
                addAllValues(value);
            }

            context.write(key, totalWordCountMap);
        }

        //addAllValues counts the frequency of the words
        private void addAllValues(MapWritable mapWritable) {

//            Set<Writable> index = mapWritable.keySet();

            for (Writable index : mapWritable.keySet()) {

                //fetch the neighbour word count value
                IntWritable fromCount = (IntWritable) mapWritable.get(index);

                //if map contain the key then calculate the total count.
                if (totalWordCountMap.containsKey(index)) {

                    IntWritable count = (IntWritable) totalWordCountMap.get(index);
                    count.set(count.get() + fromCount.get());
                }
                else {
                    //if the map does not contain the key then insert the key.
                    totalWordCountMap.put(index, fromCount);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception
    {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 1.1_Stripes");
        job.setJarByClass(Task1_1_Stripes.class);

        job.setMapperClass(Task1_1_Stripes.StripesMapper.class);
        job.setCombinerClass(Task1_1_Stripes.StripesReducer.class);
        job.setReducerClass(Task1_1_Stripes.StripesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
