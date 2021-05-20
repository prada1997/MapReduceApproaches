

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Task1_2_Pairs {

    public static class PairsRelativeOccurrenceMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {

        private WordPair wordPair = new WordPair();
        private IntWritable valueOne = new IntWritable(1);
        private IntWritable totalCount = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //removing the symbols from the text line using replaceAll method
            //splitting the text line using regex.
            String[] tokens = value.toString().trim().replaceAll("[^a-zA-Z0-9\\s]","").split("\\s+");

            //if the text line is greater than one word then
            if (tokens.length > 1) {

                //iterating over the words present in the text line
                for (int i = 0; i < tokens.length; i++) {

                    totalCount.set(0);

                    //setting the word in wordpair class as key
                    wordPair.setWord(tokens[i].trim());

                    //iterating over the words which are neighbour present in the text line
                    for (int j = 0; j < tokens.length; j++) {

                        //if the key word and neighbour is not equal
                        //then setting the word as neighbour for the key word
                        if (i != j){
                            wordPair.setNeighbor(tokens[j].trim());
                            context.write(wordPair, valueOne);
                            totalCount.set(totalCount.get() + 1);

                        }
                    }
                    //setting the * symbol and its count
                    wordPair.setNeighbor("*");

                    //emitting the word and its neighbour along with occurrence
                    context.write(wordPair, totalCount);
                }
            }
        }
    }

    public static class WordPairPartitioner extends Partitioner<WordPair,IntWritable> {

        @Override
        public int getPartition(WordPair wordPair, IntWritable intWritable, int numPartitions) {

            //Using hashcode to set the practitioner
            return wordPair.getWord().hashCode() % numPartitions;
        }
    }

    public static class PairsRelativeOccurrenceReducer extends Reducer<WordPair, IntWritable, WordPair, DoubleWritable> {

        private DoubleWritable totalWordCount = new DoubleWritable();
        private DoubleWritable countRelativeFrequency = new DoubleWritable();
        private Text presentWord = new Text("NOT_SET");
        private Text starSymbol = new Text("*");

        @Override
        protected void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            //if the neighbour is not * then
            if (!key.getNeighbor().equals(starSymbol)) {

                // calculate the relative frequency value
                int count = calculateTotalCount(values);
                countRelativeFrequency.set((double) count / totalWordCount.get());
                context.write(key, countRelativeFrequency);
            }
            else {

                //if the current word is equal to key word.
                if (key.getWord().equals(presentWord)) {
                    totalWordCount.set(totalWordCount.get() + calculateTotalCount(values));
                }
                else
                {
                    //if the current word is not equal to the key word then
                    //set the current word to key word
                    presentWord.set(key.getWord());
                    totalWordCount.set(0);
                    totalWordCount.set(calculateTotalCount(values));
                }
            }
        }

        private int calculateTotalCount(Iterable<IntWritable> values) {
            int count = 0;
            for (IntWritable index : values) {
                count += index.get();
            }
            return count;
        }
    }

    public static void main(String[] args) throws Exception
    {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 1.2_Pairs");
        job.setJarByClass(Task1_1_Pairs.class);

        job.setMapperClass(PairsRelativeOccurrenceMapper.class);
        job.setPartitionerClass(WordPairPartitioner.class);
        job.setCombinerClass(Task1_1_Pairs.PairReducer.class);
        job.setReducerClass(PairsRelativeOccurrenceReducer.class);
        job.setNumReduceTasks(5);

        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
