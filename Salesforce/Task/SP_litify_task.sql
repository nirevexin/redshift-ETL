CREATE OR REPLACE PROCEDURE litify.update_litify_task()
 LANGUAGE plpgsql
AS $$
BEGIN
    
    CREATE TEMP TABLE temp_source AS 
    SELECT ts.* 
    FROM litify.task_staging ts
    LEFT JOIN litify.task t 
        ON ts.id = t.id
    WHERE t.id IS NULL OR ts.lastmodifieddate > t.lastmodifieddate;

    
    MERGE INTO litify.task 
    USING temp_source AS source
    ON litify.task.id = source.id

    WHEN MATCHED 
    THEN UPDATE SET 
        subject = source.subject,
        activitydate = source.activitydate,
        status = source.status,
        priority = source.priority,
        ishighpriority = source.ishighpriority,
        ownerid = source.ownerid,
        description = source.description,
        isclosed = source.isclosed,
        createddate = source.createddate,
        createdbyid = source.createdbyid,
        lastmodifieddate = source.lastmodifieddate,
        lastmodifiedbyid = source.lastmodifiedbyid,
        systemmodstamp = source.systemmodstamp,
        reminderdatetime = source.reminderdatetime,
        isreminderset = source.isreminderset,
        isrecurrence = source.isrecurrence,
        in_progress_date__c = source.in_progress_date__c,
        tasksubtype = source.tasksubtype,
        completeddatetime = source.completeddatetime,
        litify_ext__status__c = source.litify_ext__status__c,
        litify_pm__default_matter_task__c = source.litify_pm__default_matter_task__c,
        litify_pm__matter_stage_activity__c = source.litify_pm__matter_stage_activity__c,
        litify_pm__associatedobjectname__c = source.litify_pm__associatedobjectname__c,
        litify_pm__completed_date__c = source.litify_pm__completed_date__c,
        litify_pm__assigneename__c = source.litify_pm__assigneename__c,
        litify_pm__matterstage__c = source.litify_pm__matterstage__c,
        litify_pm__userrolerelatedjunction__c = source.litify_pm__userrolerelatedjunction__c,
        show_on_calendar__c = source.show_on_calendar__c,
        completed_date__c = source.completed_date__c

    WHEN NOT MATCHED 
    THEN INSERT (
        id, whatid, subject, activitydate, status, priority, ishighpriority, 
        ownerid, description, isclosed, createddate, createdbyid, lastmodifieddate, 
        lastmodifiedbyid, systemmodstamp, reminderdatetime, isreminderset, 
        isrecurrence, in_progress_date__c, tasksubtype, completeddatetime, 
        litify_ext__status__c, litify_pm__default_matter_task__c, 
        litify_pm__matter_stage_activity__c, litify_pm__associatedobjectname__c, 
        litify_pm__completed_date__c, litify_pm__assigneename__c, 
        litify_pm__matterstage__c, litify_pm__userrolerelatedjunction__c, 
        show_on_calendar__c, completed_date__c
    ) VALUES (
        source.id, source.whatid, source.subject, source.activitydate, source.status, source.priority, source.ishighpriority, 
        source.ownerid, source.description, source.isclosed, source.createddate, source.createdbyid, source.lastmodifieddate, 
        source.lastmodifiedbyid, source.systemmodstamp, source.reminderdatetime, source.isreminderset, 
        source.isrecurrence, source.in_progress_date__c, source.tasksubtype, source.completeddatetime, 
        source.litify_ext__status__c, source.litify_pm__default_matter_task__c, 
        source.litify_pm__matter_stage_activity__c, source.litify_pm__associatedobjectname__c, 
        source.litify_pm__completed_date__c, source.litify_pm__assigneename__c, 
        source.litify_pm__matterstage__c, source.litify_pm__userrolerelatedjunction__c, 
        source.show_on_calendar__c, source.completed_date__c
    );

    
    DROP TABLE temp_source;

    DELETE FROM litify.task_staging;

END;
$$