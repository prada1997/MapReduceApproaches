
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

import java.io.IOException;



public class Task1_1_Pairs {


        public static class PairMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {

            private WordPair wordPair = new WordPair();
            private IntWritable valueOne = new IntWritable(1);

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                //removing the symbols from the text line using replaceAll method
                //splitting the text line using regex.
                String[] tokens = value.toString().trim().replaceAll("[^a-zA-Z0-9\\s]","").split("\\s+");

                //if the text line is greater than one word then
                if (tokens.length > 1) {

                    //iterating over the words present in the text line
                    for (int i = 0; i < tokens.length; i++) {

                        //setting the word in wordpair class as key
                        wordPair.setWord(tokens[i].trim());

                        //iterating over the words which are neighbour present in the text line
                        for (int j = 0; j < tokens.length; j++) {

                            //if the key word and neighbour is not equal
                            //then setting the word as neighbour for the key word
                            if (i != j) {
                                wordPair.setNeighbor(tokens[j].trim());

                                //emitting the word and its neighbour along with occurrence
                                context.write(wordPair, valueOne);

                            }
                        }
                    }
                }
            }
        }


        public static class PairReducer extends Reducer<WordPair,IntWritable,WordPair,IntWritable>
        {
            private IntWritable totalCount = new IntWritable();

            @Override
            protected void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
            {
                int count = 0;
                for (IntWritable value : values)
                {
                    count += value.get();
                }
                totalCount.set(count);
                context.write(key,totalCount);
            }
        }

        public static void main(String[] args) throws Exception
        {

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Task 1.1_Pairs");
            job.setJarByClass(Task1_1_Pairs.class);

            job.setMapperClass(Task1_1_Pairs.PairMapper.class);
            job.setCombinerClass(Task1_1_Pairs.PairReducer.class);
            job.setReducerClass(Task1_1_Pairs.PairReducer.class);

            job.setMapOutputKeyClass(WordPair.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(WordPair.class);
            job.setOutputValueClass(LongWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }

