package com.github.ltsopensource.core.support;

import java.util.concurrent.ConcurrentHashMap;

public class TaskTrackerGroupRecoder {

	private static final ConcurrentHashMap<String/* tasktracker identity */, String/* Group */> identityGroupMap = new ConcurrentHashMap<String, String>(
			1024);

	public static ConcurrentHashMap<String, String> getIdentityGroupMap() {
		return identityGroupMap;
	}

	public static void putTaskGroup(final String tasktrackerIdentity, final String group) {
		if (identityGroupMap.size() > 1000) {
			identityGroupMap.clear();
		}
		identityGroupMap.put(tasktrackerIdentity, group);
	}

	public static String getTaskGroup(final String tasktrackerIdentity) {
		return identityGroupMap.get(tasktrackerIdentity);
	}

}
