package org.amanzi.neo4j.csvtreeloader;

import java.util.Collections;

public class ACollections {

	public static String join(Iterable<Object> fields, String separator) {
		String result = null;
		for (Object field : fields) {
			if (result == null) {
				result = field.toString();
			} else {
				result += separator + field.toString();
			}
		}
		return result;
    }

    public static String join(Object[] fields, String separator) {
		String result = null;
		for (Object field : fields) {
			if (result == null) {
				result = field.toString();
			} else {
				result += separator + field.toString();
			}
		}
		return result;
	}
    
	public static class ArrayList<T> extends java.util.ArrayList<T> {
		private static final long serialVersionUID = 3523139348235658034L;

		public ArrayList(T[] objects) {
			Collections.addAll(this, objects);
		}

		public ArrayList(Iterable<T> objects) {
			for (T o : objects) {
				this.add(o);
			}
		}
	}
    
	public static class HashSet<T> extends java.util.HashSet<T> {
		private static final long serialVersionUID = 513972162814757442L;

		public HashSet(T[] objects) {
			Collections.addAll(this, objects);
		}

		public HashSet(Iterable<T> objects) {
			for (T o : objects) {
				this.add(o);
			}
		}
	}
    
	public static class IncMap<T> extends java.util.HashMap<T, Integer> {
		private static final long serialVersionUID = 5603849947365695582L;

		public Integer inc(T key) {
			return this.put(key, this.containsKey(key) ? (int) this.get(key) + 1 : 1);
		}
	}
}
