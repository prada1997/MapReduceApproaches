

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class WordPair implements Writable, WritableComparable<WordPair> {

        private Text word;
        private Text neighbor;


        public WordPair() {
            this.word = new Text();
            this.neighbor = new Text();
        }

        @Override
        public int compareTo(WordPair other) {
            int returnValue = this.word.compareTo(other.getWord());
            if(returnValue != 0){
                return returnValue;
            }
            if(this.neighbor.toString().equals("*")){
                return -1;
            }
            else if(other.getNeighbor().toString().equals("*")){
                return 1;
            }
            return this.neighbor.compareTo(other.getNeighbor());
        }

        @Override
        public void write(DataOutput out) throws IOException {
            word.write(out);
            neighbor.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            word.readFields(in);
            neighbor.readFields(in);
        }

        @Override
        public String toString() {
            return "{word=["+word+"]"+
                    " neighbor=["+neighbor+"]}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            WordPair wordPair = (WordPair) o;

            if (!Objects.equals(neighbor, wordPair.neighbor))
                return false;


            if (!Objects.equals(word, wordPair.word))
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int hash = word != null ? word.hashCode() : 0;
            hash = 163 * hash + (neighbor != null ? neighbor.hashCode() : 0);
            return hash;
        }

        public void setWord(String word){
            this.word.set(word);
        }
        public void setNeighbor(String neighbor){
            this.neighbor.set(neighbor);
        }
        public Text getWord() {
            return word;
        }
        public Text getNeighbor() {
            return neighbor;
        }
}

