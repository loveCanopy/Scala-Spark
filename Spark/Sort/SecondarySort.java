package Sort;

import scala.Serializable;
import scala.math.Ordered;

public class SecondarySort implements Serializable,Ordered<SecondarySort>{

	private int first;
	private int second;
	
	
	
	
	public SecondarySort() {
		
	}

	public SecondarySort(int first, int second) {
	
		this.first = first;
		this.second = second;
	}

	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public boolean $greater(SecondarySort arg0) {
		// TODO Auto-generated method stub
		if(this.first>arg0.getFirst()){
			return true;
		}else if(this.first==arg0.getFirst()&&this.getSecond()>arg0.getSecond())
		{
		return true;	
		}
		
		return false;
	}

	@Override
	public boolean $greater$eq(SecondarySort arg0) {
		// TODO Auto-generated method stub
		if(this.$greater(arg0)){
			return true;
		}else if(this.first==arg0.getFirst()&&this.getSecond()==arg0.getSecond())
		{
			return true;
		}
		
		return false;
	}

	@Override
	public boolean $less(SecondarySort arg0) {
		if(this.first<arg0.getFirst()){
			return true;
		}else if(this.first==arg0.getFirst()&&this.getSecond()<arg0.getSecond())
		{
		return true;	
		}
		
		return false;
	}

	@Override
	public boolean $less$eq(SecondarySort arg0) {
		// TODO Auto-generated method stub
		if(this.$less(arg0)){
			return true;
		}else if(this.first==arg0.getFirst()&&this.getSecond()==arg0.getSecond())
		{
			return true;
		}
		
		return false;
	}

	@Override
	public int compare(SecondarySort arg0) {
		// TODO Auto-generated method stub
		
		if(this.first-arg0.getFirst()!=0){
			return this.first-arg0.getFirst();
		}else{
			return this.second-arg0.getSecond();
		}
		
	}

	@Override
	public int compareTo(SecondarySort arg0) {
		// TODO Auto-generated method stub
		if(this.first-arg0.getFirst()!=0){
			return this.first-arg0.getFirst();
		}else{
			return this.second-arg0.getSecond();
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;
		result = prime * result + second;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SecondarySort other = (SecondarySort) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}

	
	
	
	
	
	
}
