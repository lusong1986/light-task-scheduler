package com.github.ltsopensource.jobtracker.support;

import java.util.Map;

import com.github.ltsopensource.core.commons.utils.CatUtils;
import com.github.ltsopensource.queue.domain.JobPo;

public class JobTraceUtil {

	public static void setCatMessageIds(JobPo jobPo) {
		Map<String, String> map = CatUtils.getContext();
		if (map != null) {
			for (String key : map.keySet()) {
				jobPo.setInternalExtParam(key, map.get(key));
			}
		}
	}

	public static void setNewCatMessageIds(JobPo jobPo) {
		Map<String, String> map = CatUtils.getNewContext();
		if (map != null) {
			for (String key : map.keySet()) {
				jobPo.setInternalExtParam(key, map.get(key));
			}
		}
	}

}
