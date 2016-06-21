package Sort;

import scala.Serializable;
import scala.math.Ordered;

public class Sort implements Serializable,Ordered<Sort>{

	
	private String name;
	private int scores;
	
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getScores() {
		return scores;
	}

	public void setScores(int scores) {
		this.scores = scores;
	}

	
	public Sort() {
		
	}

	public Sort(String name, int scores) {
		
		this.name = name;
		this.scores = scores;
	}

	@Override
	public boolean $greater(Sort arg0) {
		// TODO Auto-generated method stub
		if((this.name).compareTo(arg0.getName())>0){
			return true;
		}else if(this.name.equals(arg0.getName())&&this.getScores()>arg0.getScores())
		{
		return true;	
		}
		
		return false;
	}

	@Override
	public boolean $greater$eq(Sort arg0) {
		// TODO Auto-generated method stub
		if(this.$greater(arg0)){
			return true;
		}else if(this.getName().equals(arg0.getName())&&this.getScores()==arg0.getScores()){
			return true;
		}
		
		
		
		
		return false;
	}

	@Override
	public boolean $less(Sort arg0) {
		// TODO Auto-generated method stub
		if((this.name).compareTo(arg0.getName())<0){
			return true;
		}else if(this.name.equals(arg0.getName())&&this.getScores()<arg0.getScores())
		{
		return true;	
		}
		
		return false;
	}

	@Override
	public boolean $less$eq(Sort arg0) {
		// TODO Auto-generated method stub
		if(this.$less(arg0)){
			return true;
		}else if(this.getName().equals(arg0.getName())&&this.getScores()==arg0.getScores()){
			return true;
		}
		
		
		
		
		return false;
	}

	@Override
	public int compare(Sort arg0) {
		// TODO Auto-generated method stub
		if((this.getName()).compareTo(arg0.getName())!=0){
			return (this.getName()).compareTo(arg0.getName());
		}else{
			return this.getScores()-arg0.getScores();
		}
	}

	@Override
	public int compareTo(Sort arg0) {
		// TODO Auto-generated method stub
		if((this.getName()).compareTo(arg0.getName())!=0){
			return (this.getName()).compareTo(arg0.getName());
		}else{
			return this.getScores()-arg0.getScores();
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + scores;
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
		Sort other = (Sort) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (scores != other.scores)
			return false;
		return true;
	}

	
	
	
}
