package MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class LiecensesReducer extends Reducer<Text, Text, Text, Text>  {
	private Text outText= new Text();
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
		Iterator<Text> it=values.iterator();
		ArrayList<String> arr1=new ArrayList<String>();
		ArrayList<String> arr2=new ArrayList<String>();
		while (it.hasNext())
		{
			String val =it.next().toString();
			arr1.add(val);
			arr2.add(val);
		}
		
		isExistMoreThanOne(arr1,arr2,key,context);

		
	}
	private void isExistMoreThanOne(ArrayList<String> arr1,ArrayList<String> arr2,Text key,Context context) throws IOException, InterruptedException{
		ArrayList<String>toWrite=new ArrayList<String>();
		for(int i = 0;i< arr1.size();i++){
			 String val1 =  arr1.get(i);
			for(int j = 0;j< arr2.size();j++){
				String val2 =  arr2.get(j);
				 if(j != i && val1.equals(val2) ){
					
					
					  if(!toWrite.contains(val1)){
						  toWrite.add(val1);
					  }
						
				  }
			}
			
			
		}
		for(int c = 0;c< toWrite.size();c++){
			  outText.set(toWrite.get(c));
		     context.write(key, outText);
		}
	
	}
	
}
