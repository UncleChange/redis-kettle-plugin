package org.pentaho.di.trans.steps.redisfinddel;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

/**
 * The Redis Input step looks up value objects, from the given key names, from Redis server(s).
 */
public class RedisFindDel extends BaseStep implements StepInterface {
	private static Class<?> PKG = RedisFindDelMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

	protected RedisFindDelMeta meta;
	protected RedisFindDelData data;
	String keytype="string" ;
	String mastername ;
	String tablename ;
	String valueFieldName;
	public RedisFindDel(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
			Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	private static JedisSentinelPool pool = null;

	@Override
	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		if (super.init(smi, sdi)) {
			try {
				// Create client and connect to redis server(s)
				Set<Map<String,String>> jedisClusterNodes = ((RedisFindDelMeta) smi).getServers();
				// 建立连接池配置参数
				JedisPoolConfig config = new JedisPoolConfig();
				// 设置最大连接数
				config.setMaxTotal(2000);
				// 设置最大阻塞时间，记住是毫秒数milliseconds
				config.setMaxWaitMillis(1000);
				// 设置空间连接
				config.setMaxIdle(30);
				// jedis实例是否可用
				config.setTestOnBorrow(true);
				// 创建连接池
				//获取redis密码
				String password =null;
				int timeout=1000;
				String masterName =((RedisFindDelMeta) smi).getMasterName();
				Set<String> sentinels = new HashSet<String>();
				Iterator<Map<String,String>> it = jedisClusterNodes.iterator(); 
				while (it.hasNext()) {  
					Map<String,String>  hostAndPort= it.next();  
					password =hostAndPort.get("auth");
					sentinels.add(hostAndPort.get("hostname")+":"+hostAndPort.get("port"));
				}
				pool = new JedisSentinelPool(masterName, sentinels, config, timeout, password);


				return true;
			} catch (Exception e) {
				logError(BaseMessages.getString(PKG, "RedisFindDel.Error.ConnectError"), e);
				return false;
			}
		} else {
			return false;
		}
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
		Jedis jedis = pool.getResource();
		meta = (RedisFindDelMeta) smi;
		data = (RedisFindDelData) sdi;
		if (!data.hasNext) {
			setOutputDone();
			return false;
		}
		if (first) {
			first = false;
			valueFieldName = environmentSubstitute(meta.getValueFieldName());
			logBasic("valueFieldName:"+valueFieldName);
			tablename = environmentSubstitute(meta.getTableName());
            logBasic("tablename:"+tablename);
			mastername = meta.getMasterName();
			Set<String> keys = jedis.keys(tablename+"_0_*");
			data.allrow = keys.iterator();

			//复制输入行的元数据，并设置为输出行的元数据。
			//构造一个新的输出列。

			RowMetaInterface rowMeta = new RowMeta();

			Object[] rowData = new Object[1];

			int valtype = ValueMeta.getType("String");

			ValueMetaInterface valueMeta = new ValueMeta( valueFieldName, valtype);

			// 新字段名是 FileName1 ，类型是 String

			valueMeta.setLength(-1);

			rowMeta.addValueMeta(valueMeta);

			RowMetaAndData metaAndData = new RowMetaAndData(rowMeta, rowData);

			data.outputRowMeta= metaAndData.getRowMeta();
		}
		if(!data.allrow.hasNext()){
			setOutputDone();
			return false;
		}
		String key =data.allrow.next();
		data.hasNext=data.allrow.hasNext();
		String rediskey=tablename+"_id"+"_"+jedis.get(key);
		String id=jedis.get(rediskey);

		//将新的数据追加到原来的行数据的后面，成为新的输出行

		Object[] values = new Object[1];

		values[0]=id;


		//将输出行的元数据和数据放到缓存里，这样下一个步骤可以读取了，注意元数据的个数和数据的个数要相等。

		putRow(data.outputRowMeta, values);

		if (checkFeedback(getLinesRead())) {
			if (log.isBasic()) {
				logBasic(BaseMessages.getString(PKG, "RedisFindDel.Log.LineNumber") + getLinesRead());
			}
		}
		jedis.close();
		return true;
	}
	@Override
	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		super.dispose(smi, sdi);
		if(pool!=null){
			pool.close();
			pool.destroy();
		}
	}
}
