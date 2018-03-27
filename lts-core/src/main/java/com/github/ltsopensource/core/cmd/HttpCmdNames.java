package com.github.ltsopensource.core.cmd;

/**
 * @author Robert HG (254963746@qq.com) on 10/27/15.
 */
public interface HttpCmdNames {

	String HTTP_CMD_LOAD_JOB = "job_load_cmd";

	String HTTP_CMD_ADD_JOB = "job_add_cmd";

	String HTTP_CMD_TRIGGER_JOB_MANUALLY = "trigger_job_manually_cmd";

	String HTTP_CMD_ADD_M_DATA = "monitor_data_add_cmd";

	String HTTP_CMD_STATUS_CHECK = "status_check_cmd";

	String HTTP_CMD_JVM_INFO_GET = "jvm_info_get_cmd";

	String HTTP_CMD_JOB_TERMINATE = "job_terminate_cmd";

	String HTTP_CMD_JOB_PULL_MACHINE = "job_pull_machine_cmd";

	String HTTP_CMD_ADD_JOB_GRAY = "job_gray_add_cmd";

	String HTTP_CMD_UPDATE_JOB_GRAY = "job_gray_update_cmd";

	String HTTP_CMD_DEL_JOB_GRAY = "job_gray_del_cmd";

	String HTTP_CMD_ADD_JOB_DEPENDENCY = "job_dependency_add_cmd";

	String HTTP_CMD_SUSPEND_ALL_JOB = "suspend_all_job_cmd";

	String HTTP_CMD_RECOVERY_ALL_JOB = "recovery_all_job_cmd";

	String HTTP_CMD_GET_TASKTRACKER_NODE = "get_tasktracker_cmd";

	String HTTP_CMD_GET_PROCESSOR_THREAD_INFO_NODE = "get_processor_thread_info_cmd";
}
