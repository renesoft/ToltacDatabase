package Tools;

import java.util.HashMap;
import java.util.Set;

public class ThreadLocalManaged <T> {
	public static interface ThreadLocalManagedRemoveIterator <T> {
		public void removed (T t) ; 
	}
	HashMap<Long,T> map = new HashMap<>();
	
	public void set (T t) {
		long id = Thread.currentThread().getId();
		synchronized (map) {
			map.put(id, t);
		}
	}
	
	public T get () {
		long id = Thread.currentThread().getId();
		synchronized (map) {
			return map.get(id);
		}
	}
	
	public T remove () {
		long id = Thread.currentThread().getId();
		synchronized (map) {
			return map.remove(id);
		}
	}		
	
	public void removeAll (ThreadLocalManagedRemoveIterator remover) {
		synchronized (map) {
			Set<Long> set = map.keySet();
			for (Long l : set) {
				T t = map.remove(l);
				if (remover!=null)
					remover.removed(t);
			}
		}
	}
	
	public void removeAll () {
		removeAll(null);
	}
	
}
