package org.pentaho.di.trans.steps.resetredis;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.sun.xml.internal.ws.util.StringUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

/**
 * The Redis Input step looks up value objects, from the given key names, from Redis server(s).
 */
public class RedisReset extends BaseStep implements StepInterface {
    private static Class<?> PKG = RedisResetMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

    protected RedisResetMeta meta;
    protected RedisResetData data;
    String keytype="string" ;
    String key2 = "default";
    String valuetype ;
    String tablename ;
    String idfieldname ;
    public RedisReset(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                      Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    private static JedisSentinelPool pool = null;


    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        if (super.init(smi, sdi)) {
            try {
                // Create client and connect to redis server(s)
                Set<Map<String,String>> jedisClusterNodes = ((RedisResetMeta) smi).getServers();
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
                String masterName =((RedisResetMeta) smi).getMasterName();
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
                logError(BaseMessages.getString(PKG, "RedisReset.Error.ConnectError"), e);
                return false;
            }
        } else {
            return false;
        }
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        meta = (RedisResetMeta) smi;
        data = (RedisResetData) sdi;
        String param1="*_1_*";
        String param0="*_0_*";
        if(meta.getTableName()!=null&&!StringUtil.isEmpty(meta.getTableName())){
        	tablename = environmentSubstitute(meta.getTableName());
        	param1=tablename+"_1_*";
        	param0=tablename+"_0_*";
        }
        logBasic("tablename:"+tablename);
        Jedis jedis = pool.getResource();
        //先删除所有状态为0 数据（即删除或者修改的数据）
        Set<String> keysdel = jedis.keys(param0);
        Iterator<String> iteratordel = keysdel.iterator();
        while (iteratordel.hasNext()) {
        	String key =iteratordel.next();
        	jedis.del(key);
		}
        //将全部状态都重置为0
        Set<String> keys = jedis.keys(param1);
        Iterator<String> iterator = keys.iterator();
        while (iterator.hasNext()) {
        	String key =iterator.next();
        	String newkey=key.replaceAll("_1_", "_0_");
        	jedis.set(newkey, jedis.get(key));
        	jedis.del(key);
		}
        if (checkFeedback(getLinesRead())) {
            if (log.isBasic()) {
                logBasic(BaseMessages.getString(PKG, "RedisOutput.Log.LineNumber") + getLinesRead());
            }
        }
        jedis.close();
        setOutputDone();
        return false;
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
