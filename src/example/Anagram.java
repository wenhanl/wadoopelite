package example;

import mapr.MapReduceJob;
import mapr.Mapper;
import mapr.Record;
import mapr.Reducer;

import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by wenhanl on 14-11-21.
 */
public class Anagram extends MapReduceJob<Integer, String, String, String>{

    /**
     * The Anagram mapper class gets a word as a line from the HDFS input and sorts the
     * letters in the word and writes its back to the output collector as
     * Key : sorted word (letters in the word sorted)
     * Value: the word itself as the value.
     * When the reducer runs then we can group anagrams togather based on the sorted key.
     *
     * @author Wenhan Lu
     *
     */
    public Mapper<Integer, String, String, String> getMapper() {
        return new Mapper<Integer, String, String, String>() {
            public void map(Integer key, String value) {
                char[] wordChars = value.toCharArray();
                Arrays.sort(wordChars);
                String sortedWord = new String(wordChars);
                output.add(new Record<String, String>(sortedWord, value));
            }
        };
    }

    /**
     * The Anagram reducer class groups the values of the sorted keys that came in and
     * checks to see if the values iterator contains more than one word. if the values
     * contain more than one word we have spotted a anagram.
     * @author Wenhan Lu
     *
     */
    public Reducer<String, String, String, String> getReducer() {
        return new Reducer<String, String, String, String>() {
            //the value of the same key is put in a list
            public void reduce(String key, List<String> values) {
                String out = "";
                for(String value : values){
                    out = out + value + "~";
                }
                StringTokenizer outputTokenizer = new StringTokenizer(out,"~");
                if(outputTokenizer.countTokens()>=2){
                    out = out.replace("~", ";");
                    output.add(new Record<String, String>(key, out));
                }
            }
        };
    }
}
