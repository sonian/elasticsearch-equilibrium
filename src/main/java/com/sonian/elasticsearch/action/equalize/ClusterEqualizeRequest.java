package com.sonian.elasticsearch.action.equalize;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;

/**
 * @author dakrone
 */
public class ClusterEqualizeRequest extends MasterNodeOperationRequest {
    public ClusterEqualizeRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
