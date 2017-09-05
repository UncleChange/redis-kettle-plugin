package org.pentaho.di.trans.steps.resetredis;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;


/**
 * @author Matt Burgess
 */
public class RedisResetData extends BaseStepData implements StepDataInterface {

    public RowMetaInterface outputRowMeta;

    /**
     *
     */
    public RedisResetData() {
        super();
    }

}
