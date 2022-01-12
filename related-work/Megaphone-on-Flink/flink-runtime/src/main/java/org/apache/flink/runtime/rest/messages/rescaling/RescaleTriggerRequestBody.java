package org.apache.flink.runtime.rest.messages.rescaling;

import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;

public class RescaleTriggerRequestBody implements RequestBody {

    public static final String FIELD_NAME_RESCALE_MODE = "rescale-mode";

    @JsonCreator
    public RescaleTriggerRequestBody() {

    }


}

