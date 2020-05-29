/**
 * @author Dzenan Hamzic
 *
 */
import java.io.IOException;
import java.text.Normalizer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class WordCount {
 
    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable> {
 
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
 
       
		public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
			
				/*
			
				//Questão 2a. Quais foram as hashtags mais usadas pela manhã, tarde e noite?
				
                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
                	
      				String token = itr.nextToken();
                    token = token.toLowerCase();
                    token = token.replace("\"", "");
                    
                    
                    if (token.startsWith("#")) {
                	
                    	String aux = itr.nextToken();
                    	aux = aux.replace("\"", "");
                    	
                    	boolean cond = true;
                    	String filter = aux;
                    	
                    	
                    	do {
                    		
                    		if(Pattern.matches("\\d{2}:\\d{2}:\\d{2}", aux)) {
                    			//aux = itr.nextToken();
                    			filter = aux;
                    			cond=false;
                    		}
                    		aux = itr.nextToken();
                    		
                    	}while(cond);
                    	
                    	LocalTime time = LocalTime.parse(filter, 
                        DateTimeFormatter.ISO_TIME);
                       
                        if(time.getHour() > 12 && time.getHour() < 18) { // Caso queira os tweets de manhã. Alterar a para > 6 && < 12, para noite > 18 || < 6
                        	System.out.println(filter);
                        	 word.set(token);
                             context.write(word, one);
                        }
                    }
                }
                */
			
			/*
              
			  //Questão 2b. Quais as hashtags mais usadas em cada dia?
			
			  StringTokenizer itr = new StringTokenizer(value.toString());
              while (itr.hasMoreTokens()) {
              	
    				String token = itr.nextToken();
                  token = token.toLowerCase();
                  token = token.replace("\"", "");
                  
                  
                  if (token.startsWith("#")) {
                	  String aux = token;
                	  boolean cond = true;
                	  
                	  while(cond && itr.hasMoreTokens()) {
                		  token = itr.nextToken();
                		  token = token.replace("\"", "");
                		  
                		  if (Pattern.matches("\\d{4}-\\d{2}-\\d{2}", token)) {
                			  if(token.equals("2014-10-15")) { //Colocar os dias(2014-10-16, 2014-10-17, 2014-10-18, 2014-10-19, 2014-10-20)
                				  word.set(aux);
                				  context.write(word, one);
                			  }
                		  }
                	  }
			
                  }
              }	
             */
                  
			/*
                //Questão 2d. Quais as principais sentenças relacionadas à palavra “Dilma”?
                //Questão 2e. Quais as principais sentenças relacionadas à palavra “Aécio”?
                 
                 
				StringTokenizer itr = new StringTokenizer(value.toString());	
                while (itr.hasMoreTokens()) {
                	String token = itr.nextToken();
                    token = token.toLowerCase();
                    
	                if (token.startsWith("\"") && !(token.endsWith("\""))) {
	                    	
	                	String setenca = token;
	                    	
	                   	token = itr.nextToken();
	                    token = token.toLowerCase();
	                    while(itr.hasMoreTokens() && !token.endsWith("\"")) {
	                    	setenca = setenca+" "+token;
	                    	token = itr.nextToken();
	                        token = token.toLowerCase();
	                    }
	                    if(token.endsWith("\""))
	                    	setenca = setenca+" "+token;
	                    	
	                    if(setenca.contains("dilma") || setenca.contains("dilma")) { //substituir a palavra por "dilma"
	                    	word.set(setenca);
	            	    	context.write(word, one);
	                    }
	                    	
	                    
	                }
                }
            */
			
			/*
			
			//Questão 3a. Encontre as palavras mais utilizadas nas avaliações.
			
			String[] values = value.toString().split(",\"", -1);
			
			for(int i=0; i < values.length;i++) {
				
				String[] keyValue = values[i].toString().split(":\"", -1);
				
				for(int j=0;j < keyValue.length; j++) {
					
					keyValue[j] = keyValue[j].toLowerCase();
					keyValue[j] = keyValue[j].replace("\"", "");
					if(keyValue[j].equals("text")) {
						String palavra = keyValue[j+1];
						palavra = palavra.toLowerCase();
						
						 StringTokenizer itr = new StringTokenizer(palavra);
						 while(itr.hasMoreTokens()) {
							 String token = itr.nextToken();
							 token = token.replace(",", "");
							 token = token.replace(".", "");
							 token = token.replace("\"", "");
							 token = token.replace("\'", "");
							 word.set(token);
                             context.write(word, one);
						 }
					}
				}
			}
			*/
			
			
			/*
			 
			//Questão 3b. Encontre as expressões mais usadas. Considere uma expressão um conjunto de palavras na sequencia. O tamanho da sequencia pode ser determinado por você.
			
			String[] values = value.toString().split(",\"", -1);
			
			for(int i=0; i < values.length;i++) {
				
				String[] keyValue = values[i].toString().split(":\"", -1);
				
				for(int j=0;j < keyValue.length; j++) {
					
					keyValue[j] = keyValue[j].toLowerCase();
					keyValue[j] = keyValue[j].replace("\"", "");
					if(keyValue[j].equals("text")) {
						String palavra = keyValue[j+1];
						palavra = palavra.toLowerCase();
						
						 StringTokenizer itr = new StringTokenizer(palavra);
						 
						 while(itr.hasMoreTokens()) {
							 String token = itr.nextToken();
							 int count=0;
							 while(itr.hasMoreTokens() && count < 2) {
								 token = token+" "+itr.nextToken();
								 token = token.replace(",", "");
								 token = token.replace(".", "");
								 token = token.replace("\"", "");
								 token = token.replace("\'", "");
								 count += 1;
						 	}
							 
							System.out.println(token);
							word.set(token);
                            context.write(word, one);
							
						 }
					}
				}
			}
			*/
            
			/*
			//Questão 3c. Encontre os principais tópicos relacionados às revisões.
			
			String[] values = value.toString().split(",\"", -1);
			
			for(int i=0; i < values.length;i++) {
				
				String[] keyValue = values[i].toString().split(":\"", -1);
				
				for(int j=0;j < keyValue.length; j++) {
					
					keyValue[j] = keyValue[j].toLowerCase();
					keyValue[j] = keyValue[j].replace("\"", "");
					
					if(keyValue[j].equals("title")) {
						
						String palavra = keyValue[j+1];
						palavra = palavra.toLowerCase();
						palavra = palavra.replace("\"", "");
						palavra = palavra.replace("!", "");
						
						word.set(palavra);
                        context.write(word, one);
					}
				}
			}
			*/
			
			/*
			
			//Questão 3d. Mapeie a distribuição temporal das revisões.
			
			String[] values = value.toString().split(",\"", -1);
			
			for(int i=0; i < values.length;i++) {
				
				String[] keyValue = values[i].toString().split(":\"", -1);
				
				for(int j=0;j < keyValue.length; j++) {
					
					keyValue[j] = keyValue[j].toLowerCase();
					keyValue[j] = keyValue[j].replace("\"", "");
					
					if(keyValue[j].equals("createdat")) {
						
						String palavra = keyValue[j+1];
						palavra = palavra.toLowerCase();
						palavra = palavra.replace("\"", "");
						palavra = palavra.replace("!", "");
						
						word.set(palavra);
                        context.write(word, one);
					}
				}
			}
		*/
 
        }
    }
 
    /**
     * Reducer
     * it sums values for every hashtag and puts them to HashMap
     */
    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
 
        private Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();
 
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
 
            // computes the number of occurrences of a single word
            int sum = 0;
            
            for (IntWritable val : values) {
                sum += val.get();
            }
 
            // puts the number of occurrences of this word into the map.
            // We need to create another Text object because the Text instance
            // we receive is the same for all the words
            countMap.put(new Text(key), new IntWritable(sum));
        }
 
        /**
         * this mehtod is run after the reducer has seen all the values.
         * it is used to output top 15 hashtags in file.
         */
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
 
            Map<Text, IntWritable> sortedMap = sortByValues(countMap);
 
            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 150) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }
 
    /**
     * This mehtod sorts Map by values
     * @param map
     * @return sortedMap Map<K,V>;
     */
    private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(
            Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(
                map.entrySet());
 
        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
 
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
 
        // LinkedHashMap will keep the keys in the order they are inserted
        // which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();
 
        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
 
        return sortedMap;
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(TopNCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}