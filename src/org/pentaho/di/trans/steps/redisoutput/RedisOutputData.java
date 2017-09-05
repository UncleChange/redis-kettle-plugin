package org.pentaho.di.trans.steps.redisoutput;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;


/**
 * @author Matt Burgess
 */
public class RedisOutputData extends BaseStepData implements StepDataInterface {

    public RowMetaInterface outputRowMeta;

    /**
     *
     */
    public RedisOutputData() {
        super();
    }

}
