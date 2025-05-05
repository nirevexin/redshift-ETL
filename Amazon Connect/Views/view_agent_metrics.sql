CREATE OR REPLACE VIEW connect.view_agent_metrics AS
SELECT
CAST(c.agent_conn as DATE) AS call_date,
EXTRACT(MONTH FROM c.agent_conn) AS call_month,
EXTRACT(DAY FROM c.agent_conn) AS call_day,
EXTRACT(HOUR FROM c.agent_conn) AS call_hour,
u.user_name,
u.user_lastname,
u.user_name ||' '|| u.user_lastname AS user_complete_name,
u.user_email,
l.title,
l.cm_job_title__c AS cm_job_title ,
l.department__c AS department,
q.queue_name,
SUM(c.agent_interact_duration) AS total_agent_interaction,
ROUND(SUM(c.agent_interact_duration) / 3600::FLOAT, 2) AS total_agent_interaction_hours,
SUM(c.agent_interact_duration) / 60::INT AS total_agent_interaction_minutes,
AVG(c.agent_interact_duration) AS avg_agent_interaction,
SUM(EXTRACT(EPOCH FROM c.disconn_time - c.agent_conn)) AS total_contact_duration,
ROUND(SUM(EXTRACT(EPOCH FROM c.disconn_time - c.agent_conn)) / 3600::FLOAT,2) AS total_contact_duration_hours,
ROUND(SUM(EXTRACT(EPOCH FROM c.disconn_time - c.agent_conn)) / 60::INT) AS total_contact_duration_minutes,
AVG(EXTRACT(EPOCH FROM c.disconn_time - c.agent_conn)) AS avg_contact_duration,
SUM(c.agent_afw_duration) AS total_agent_afw_duration,
ROUND(SUM(c.agent_afw_duration) / 3600::FLOAT,2) AS total_agent_afw_duration_hours,
SUM(c.agent_afw_duration) / 60::INT AS total_agent_afw_duration_minutes,
AVG(c.agent_afw_duration) AS avg_agent_afw_duration,
MIN(c.agent_longest_hold) AS min_agent_longest_hold,
MAX(c.agent_longest_hold) AS max_agent_longest_hold,
SUM(c.agent_conn_att) AS total_agent_conn_attempts,
SUM(c.customer_hold_duration) AS total_customer_hold_duration,
ROUND(SUM(c.customer_hold_duration) / 3600::FLOAT,2) AS total_customer_hold_duration_hours,
SUM(c.customer_hold_duration) / 60::INT AS total_customer_hold_duration_minutes,
AVG(c.customer_hold_duration) AS avg_customer_hold_duration,
COUNT(DISTINCT c.customer_phone) AS unique_customers,
COUNT(c.agent_conn) AS total_calls,
COUNT(
        CASE
        WHEN c.agent_interact_duration BETWEEN 0 AND 120 THEN 1
        ELSE NULL END
    ) AS duration_less_2_minutes,
COUNT(
        CASE
        WHEN c.agent_interact_duration BETWEEN 120 AND 300 THEN 1
        ELSE NULL END
    ) AS duration_2_to_5_minutes,
COUNT(
        CASE
        WHEN c.agent_interact_duration BETWEEN 360 AND 1200 THEN 1
        ELSE NULL END
    ) AS duration_6_to_20_minutes,
COUNT(
        CASE
        WHEN c.agent_interact_duration BETWEEN 1260 AND 2700 THEN 1
        ELSE NULL END
    ) AS duration_21_to_45_minutes,
COUNT(
        CASE
        WHEN c.agent_interact_duration BETWEEN 2760 AND 5400 THEN 1
        ELSE NULL END
    ) AS duration_46_to_90_minutes,
COUNT(
        CASE
        WHEN c.agent_interact_duration > 5400 THEN 1
        ELSE NULL END
    ) AS duration_more_than_90_minutes,

COUNT(
    CASE
        WHEN EXTRACT(HOUR FROM c.agent_conn) BETWEEN 9 AND 13 
             AND EXTRACT(HOUR FROM c.disconn_time) <= 13 THEN 1
        ELSE NULL END
) AS calls_9_13,

COUNT(
    CASE
        WHEN EXTRACT(HOUR FROM c.agent_conn) BETWEEN 13 AND 17 
             AND EXTRACT(HOUR FROM c.disconn_time) <= 17 THEN 1
        ELSE NULL END
) AS calls_13_17,

COUNT(
    CASE
        WHEN EXTRACT(HOUR FROM c.agent_conn) >= 17 
             AND EXTRACT(HOUR FROM c.disconn_time) >= 17 THEN 1
        ELSE NULL END
) AS calls_17_00,

COUNT(
    CASE
    WHEN c.init_method = 'INBOUND' THEN 1
    ELSE NULL END
) AS inbound_calls,

COUNT(
    CASE
    WHEN c.init_method = 'OUTBOUND' THEN 1
    ELSE NULL END
) AS outbound_calls,

COUNT(
    CASE
    WHEN c.init_method = 'TRANSFER' THEN 1
    ELSE NULL END
) AS transfer_calls,

COUNT(
    CASE
    WHEN c.init_method = 'CALLBACK' THEN 1
    ELSE NULL END
) AS callback_calls

FROM
connect.f_calls AS c 
LEFT JOIN connect.dim_users AS u ON c.agent_id = u.user_id
LEFT JOIN connect.dim_queues AS q ON c.queue_id = q.queue_id 
LEFT JOIN litify.dim_users AS l ON c.agent_username = l.username
WHERE 
    CAST(c.agent_conn as DATE) IS NOT NULL
GROUP BY
    CAST(c.agent_conn AS DATE),
    EXTRACT(MONTH FROM c.agent_conn),
    EXTRACT(DAY FROM c.agent_conn),
    EXTRACT(HOUR FROM c.agent_conn),
    u.user_name,
    u.user_lastname,
    user_complete_name,
    u.user_email,
    q.queue_name,
    l.title,
    l.cm_job_title__c,
    l.cm_job_title_multi__c,
    l.department__c
ORDER BY 
    call_date,
    call_month,
    call_day,
    call_hour DESC;