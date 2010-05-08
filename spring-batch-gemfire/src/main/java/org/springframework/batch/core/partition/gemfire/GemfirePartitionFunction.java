package org.springframework.batch.core.partition.gemfire;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.StepExecution;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;

public class GemfirePartitionFunction extends FunctionAdapter {

	private static Log logger = LogFactory.getLog(GemfirePartitionFunction.class);

	@Override
	public void execute(FunctionContext context) {
		execute(context.<StepExecution> getResultSender(), extract((RegionFunctionContext) context));
	}

	private void execute(ResultSender<StepExecution> sender, List<RemoteStepExecutor> executors) {
		int count = 1;
		for (RemoteStepExecutor executor : executors) {
			count++;
			if (count < executors.size()) {
				sender.sendResult(executor.execute());
			} else {
				sender.lastResult(executor.execute());
			}
		}
	}

	private List<RemoteStepExecutor> extract(RegionFunctionContext context) {
		Map<String, RemoteStepExecutor> data = PartitionRegionHelper.getLocalDataForContext(context);
		List<RemoteStepExecutor> list = new ArrayList<RemoteStepExecutor>();
		for (Entry<String, RemoteStepExecutor> entry : data.entrySet()) {
			logger.debug("Extract:" + entry);
			list.add(entry.getValue());
		}
		return list;
	}

	@Override
	public String getId() {
		return getClass().getSimpleName();
	}

}
