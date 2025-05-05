CREATE OR REPLACE PROCEDURE legal.update_employee()
 LANGUAGE plpgsql
AS $$
BEGIN
    
    CREATE TEMP TABLE temp_source_emp AS
    SELECT es.*
    FROM legal.employee_staging es
    LEFT JOIN legal.employee e
        ON es.email = e.email
    WHERE e.email IS NULL OR es.lastmodifieddate > e.lastmodifieddate;

    
    MERGE INTO legal.employee
    USING temp_source_emp AS source
    ON legal.employee.email = source.email

    
    WHEN MATCHED THEN
        UPDATE SET 
            name = source.name,
            position = source.position,
            tower = source.tower,
            team = source.team,
            supervisor = source.supervisor,
            manager = source.manager,
            hire = source.hire,
            fire = source.fire,
            country = source.country,
            birth = source.birth,
            phone = source.phone,
            work_phone = source.work_phone,
            schedule_daylight = source.schedule_daylight,
            schedule_standard = source.schedule_standard,
            lastmodifieddate = source.lastmodifieddate

    
    WHEN NOT MATCHED THEN
        INSERT (
            email, name, position, tower, team, supervisor, manager, hire, fire, country, birth, phone, work_phone, schedule_daylight, schedule_standard, lastmodifieddate
        ) 
        VALUES (
            source.email, source.name, source.position, source.tower, source.team, source.supervisor, source.manager, source.hire, source.fire, source.country, source.birth, source.phone, source.work_phone, source.schedule_daylight, source.schedule_standard, source.lastmodifieddate
        );

    
    DROP TABLE temp_source_emp;

    
    DELETE FROM legal.employee_staging;

END;
$$