CREATE TABLE os_custom_reports (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    file_path VARCHAR(255) NOT NULL,
    config_file_path VARCHAR(255) NOT NULL
);

INSERT INTO os_custom_reports (file_path, config_file_path)
VALUES ('/usr/local/openspecimen/os-prod/custom-codes/form-audit-report/form-audit-report.py', '/usr/local/openspecimen/os-prod/custom-codes/form-audit-report/config.properties');

CREATE TABLE `os_custom_reports_on_demand_runs` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `script_id` bigint DEFAULT NULL,
  `job_end_time` datetime DEFAULT NULL,
  `command_line_parameters` varchar(500) COLLATE utf8mb3_unicode_ci DEFAULT NULL,
  `error_message` text COLLATE utf8mb3_unicode_ci,
  `job_status` varchar(50) COLLATE utf8mb3_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `report_id` (`script_id`),
  CONSTRAINT `os_custom_reports_on_demand_runs_ibfk_1` FOREIGN KEY (`script_id`) REFERENCES `os_custom_reports` (`id`)
)

INSERT INTO os_custom_reports_on_demand_runs (script_id) VALUES (1);
