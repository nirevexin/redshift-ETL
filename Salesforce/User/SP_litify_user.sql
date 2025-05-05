CREATE OR REPLACE PROCEDURE litify.update_litify_user()
 LANGUAGE plpgsql
AS $$
BEGIN
    
    CREATE TEMP TABLE temp_source_u AS 
    SELECT us.* 
    FROM litify.dim_users_staging us
    LEFT JOIN litify.dim_users u 
        ON us.id = u.id
    WHERE u.id IS NULL OR us.lastmodifieddate > u.lastmodifieddate;

    
    MERGE INTO litify.dim_users 
    USING temp_source_u AS source
    ON litify.dim_users.id = source.id

    WHEN MATCHED THEN UPDATE SET
        username = source.username,
        alias = source.alias,
        communitynickname = source.communitynickname,
        firstname = source.firstname,
        lastname = source.lastname,
        title = source.title,
        cm_job_title__c = source.cm_job_title__c,
        cm_job_title_multi__c = source.cm_job_title_multi__c,
        department__c = source.department__c,
        isactive = source.isactive,
        startday = source.startday,
        endday = source.endday,
        companyname = source.companyname,
        timezonesidkey = source.timezonesidkey,
        localesidkey = source.localesidkey,
        usertype = source.usertype,
        passwordexpirationdate = source.passwordexpirationdate,
        systemmodstamp = source.systemmodstamp,
        lastpasswordchangedate = source.lastpasswordchangedate,
        createddate = source.createddate,
        createdbyid = source.createdbyid,
        lastmodifieddate = source.lastmodifieddate,
        lastmodifiedbyid = source.lastmodifiedbyid,
        lastlogindate = source.lastlogindate,
        receivesinfoemails = source.receivesinfoemails,
        receivesadmininfoemails = source.receivesadmininfoemails,
        numberoffailedlogins = source.numberoffailedlogins,
        dfsle__username__c = source.dfsle__username__c,
        dfsle__status__c = source.dfsle__status__c,
        dfsle__provisioned__c = source.dfsle__provisioned__c,
        dfsle__canmanageaccount__c = source.dfsle__canmanageaccount__c,
        aboutme = source.aboutme,
        federationidentifier = source.federationidentifier,
        attorneys_per_page__c = source.attorneys_per_page__c,
        lastreferenceddate = source.lastreferenceddate,
        lastvieweddate = source.lastvieweddate,
        defaultgroupnotificationfrequency = source.defaultgroupnotificationfrequency,
        digestfrequency = source.digestfrequency,
        profileid = source.profileid

    WHEN NOT MATCHED THEN INSERT (
        id, username, alias, communitynickname, firstname, lastname, title,
        cm_job_title__c, cm_job_title_multi__c, department__c, isactive, startday, endday,
        companyname, timezonesidkey, localesidkey, usertype, passwordexpirationdate,
        systemmodstamp, lastpasswordchangedate, createddate, createdbyid,
        lastmodifieddate, lastmodifiedbyid, lastlogindate, receivesinfoemails,
        receivesadmininfoemails, numberoffailedlogins, dfsle__username__c,
        dfsle__status__c, dfsle__provisioned__c, dfsle__canmanageaccount__c,
        aboutme, federationidentifier, attorneys_per_page__c, lastreferenceddate,
        lastvieweddate, defaultgroupnotificationfrequency, digestfrequency, profileid
    ) VALUES (
        source.id, source.username, source.alias, source.communitynickname, source.firstname, source.lastname, source.title,
        source.cm_job_title__c, source.cm_job_title_multi__c, source.department__c, source.isactive, source.startday, source.endday,
        source.companyname, source.timezonesidkey, source.localesidkey, source.usertype, source.passwordexpirationdate,
        source.systemmodstamp, source.lastpasswordchangedate, source.createddate, source.createdbyid,
        source.lastmodifieddate, source.lastmodifiedbyid, source.lastlogindate, source.receivesinfoemails,
        source.receivesadmininfoemails, source.numberoffailedlogins, source.dfsle__username__c,
        source.dfsle__status__c, source.dfsle__provisioned__c, source.dfsle__canmanageaccount__c,
        source.aboutme, source.federationidentifier, source.attorneys_per_page__c, source.lastreferenceddate,
        source.lastvieweddate, source.defaultgroupnotificationfrequency, source.digestfrequency, source.profileid
    );

    
    DROP TABLE temp_source_u;
    DELETE FROM litify.dim_users_staging;
END;
$$