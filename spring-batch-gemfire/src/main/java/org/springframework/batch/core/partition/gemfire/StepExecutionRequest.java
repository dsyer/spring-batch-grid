package org.springframework.batch.core.partition.gemfire;

import java.io.Serializable;

public class StepExecutionRequest implements Serializable {
	
	private final String stepName;

	private final String contextLocation;

	public StepExecutionRequest(String stepName, String contextLocation) {
		super();
		this.stepName = stepName;
		this.contextLocation = contextLocation;
	}
	
	public String getStepName() {
		return stepName;
	}
	
	public String getContextLocation() {
		return contextLocation;
	}

	@Override
	public String toString() {
		return "StepExecutionRequest [contextLocation=" + contextLocation + ", stepName=" + stepName + "]";
	}

}
