package Tools;

import java.util.Map;

public class Pair<T, T1> implements Map.Entry<T, T1> {
	T m_First;
	T1 m_Second;

	public Pair() {

	}

	public Pair(T f, T1 l) {
		m_First = f;
		m_Second = l;
	}

	public T first() {
		return m_First;
	}

	public T1 second() {
		return m_Second;
	}

	public Pair<String, String> make(String f, String l) {
		Pair<String, String> ret = new Pair<String, String>();
		ret.m_First = f;
		ret.m_Second = l;
		return ret;
	}

	public void setFirst(T first) {
		this.m_First = first;
	}

	public void setSecond(T1 second) {
		this.m_Second = second;
	}

	  @Override
	    public T getKey() {
	        return m_First;
	    }

	    @Override
	    public T1 getValue() {
	        return m_Second;
	    }

	    @Override
	    public T1 setValue(T1 value) {
	        T1 old = this.m_Second;
	        this.m_Second = value;
	        return old;
	    }
	    
	    @Override
	    public String toString () {
	    	return ""+m_First+","+m_Second;
	    }
}
