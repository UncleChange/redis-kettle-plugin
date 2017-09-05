package org.pentaho.di.trans.steps.redisfinddel;

import java.util.Iterator;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;


/**
 * @author Matt Burgess
 */
public class RedisFindDelData extends BaseStepData implements StepDataInterface {

    public RowMetaInterface outputRowMeta;
    
    public Iterator<String> allrow;
    public boolean hasNext;

    /**
     *
     */
    public RedisFindDelData() {
        super();
        allrow=null;
        hasNext=true;
    }

}
