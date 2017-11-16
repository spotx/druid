package io.druid.data.input.avro.processor.utils;

import com.google.common.collect.ForwardingMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by pcunningham on 14/11/2017.
 */
public class DelegatingMap<K, V> extends ForwardingMap<K, V>{

    private Map<K, V> data;
    private Map<K, V> delegate;

    public DelegatingMap(Map<K,V> delegate) {
      this.data = new HashMap<K,V>();
      this.delegate = delegate;
    }

    public DelegatingMap(Map<K,V> delegate, K key, V value) {

      this(delegate);

      this.data.put(key,value);
    }

    @Override
    protected Map<K,V> delegate() {
      return this.delegate;
    }

    @Override
    public V get(Object key)
    {
      if( this.data.containsKey(key) ) {

        return data.get(key);
      }

      return this.delegate().get(key);
    }

    @Override
    public V put(K key, V value)
    {
      return data.put(key, value);
    }
}
