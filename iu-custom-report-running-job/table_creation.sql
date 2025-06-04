CREATE TABLE os_custom_reports (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    file_path VARCHAR(255) NOT NULL,
    config_file_path VARCHAR(255) NOT NULL
);

INSERT INTO os_custom_reports (file_path, config_file_path)
VALUES ('/usr/local/openspecimen/os-prod/custom-codes/form-audit-report/form-audit-report.py', '/usr/local/openspecimen/os-prod/custom-codes/form-audit-report/config.properties');

CREATE TABLE os_custom_reports_on_demand_runs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    report_id BIGINT,
    datetime DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (report_id) REFERENCES os_custom_reports(id)
);

INSERT INTO os_custom_reports_on_demand_runs (script_id) VALUES (1);
