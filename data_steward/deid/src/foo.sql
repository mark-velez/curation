SELECT * FROM (SELECT a.observation_id,a.person_id,a.observation_concept_id,a.observation_type_concept_id,a.value_as_number,a.value_as_string,a.value_as_concept_id,a.qualifier_concept_id,a.unit_concept_id,a.provider_id,a.visit_occurrence_id,a.observation_source_value,a.observation_source_concept_id,a.unit_source_value,a.qualifier_source_value,a.value_source_concept_id,a.value_source_value,a.questionnaire_response_id,observation_date,observation_datetime FROM ( SELECT observation_id,person_id,observation_concept_id,observation_type_concept_id,value_as_number,value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,observation_source_value,observation_source_concept_id,unit_source_value,qualifier_source_value,value_source_concept_id,value_source_value,questionnaire_response_id from (
                    SELECT observation_id,person_id,observation_concept_id,observation_type_concept_id,value_as_number,value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,observation_source_value,observation_source_concept_id,unit_source_value,qualifier_source_value,value_source_concept_id,value_source_value,questionnaire_response_id
                    FROM raw.observation
                

                        WHERE observation_source_concept_id in (
                            SELECT concept_id 
                            FROM raw.concept 
                            WHERE vocabulary_id = 'PPI' AND concept_class_id in ('Question','PPI Modifier')
                            
                            AND REGEXP_CONTAINS(concept_code,'(Date|Orientation|Language_SpokenWrittenLanguage|Gender|BiologicalSexAtBirth_SexAtBirth|Race_WhatRace|EducationLevel_HighestGrade|_EmploymentStatus)') IS FALSE
                        )

                       
                      UNION ALL SELECT observation_id,person_id,observation_concept_id,observation_type_concept_id,value_as_number,IF(value_source_concept_id not in (1585900),'SexualOrientation_None',value_as_string) as value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,IF(value_source_concept_id not in (1585900), 'SexualOrientation_None',observation_source_value) as observation_source_value,IF(value_source_concept_id not in (1585900),1585904,observation_source_concept_id) as observation_source_concept_id,unit_source_value,qualifier_source_value,value_source_concept_id,IF(value_source_concept_id not in (1585900), 'SexualOrientation_None',value_source_value) as value_source_value,questionnaire_response_id FROM raw.observation WHERE observation_source_concept_id in (SELECT concept_id FROM raw.concept WHERE REGEXP_CONTAINS(concept_code,'(?i)orientation'))   UNION ALL SELECT observation_id,person_id,observation_concept_id,observation_type_concept_id,value_as_number,IF(value_source_concept_id not in (1585428,1585430,1585425,1585426,1585424,1585427,1585423,1585429),'Unknown',value_as_string) as value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,IF(value_source_concept_id not in (1585428,1585430,1585425,1585426,1585424,1585427,1585423,1585429), 'Unknown',observation_source_value) as observation_source_value,IF(value_source_concept_id not in (1585428,1585430,1585425,1585426,1585424,1585427,1585423,1585429),0,observation_source_concept_id) as observation_source_concept_id,unit_source_value,qualifier_source_value,value_source_concept_id,IF(value_source_concept_id not in (1585428,1585430,1585425,1585426,1585424,1585427,1585423,1585429), 'Unknown',value_source_value) as value_source_value,questionnaire_response_id FROM raw.observation WHERE observation_source_concept_id in (SELECT concept_id FROM raw.concept WHERE REGEXP_CONTAINS(concept_code,'(?i)language'))   UNION ALL SELECT observation_id,person_id,observation_concept_id,observation_type_concept_id,value_as_number,IF(value_source_concept_id not in (8551,8570,1585840,1585839),'OTHER',value_as_string) as value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,IF(value_source_concept_id not in (8551,8570,1585840,1585839), 'OTHER',observation_source_value) as observation_source_value,IF(value_source_concept_id not in (8551,8570,1585840,1585839),8521,observation_source_concept_id) as observation_source_concept_id,unit_source_value,qualifier_source_value,value_source_concept_id,IF(value_source_concept_id not in (8551,8570,1585840,1585839), 'OTHER',value_source_value) as value_source_value,questionnaire_response_id FROM raw.observation WHERE observation_source_concept_id in (SELECT concept_id FROM raw.concept WHERE REGEXP_CONTAINS(concept_code,'(?i)gender'))   UNION ALL SELECT observation_id,person_id,observation_concept_id,observation_type_concept_id,value_as_number,IF(value_source_concept_id not in (1585846,1585847),'Unknown',value_as_string) as value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,IF(value_source_concept_id not in (1585846,1585847), 'Unknown',observation_source_value) as observation_source_value,IF(value_source_concept_id not in (1585846,1585847),0,observation_source_concept_id) as observation_source_concept_id,unit_source_value,qualifier_source_value,value_source_concept_id,IF(value_source_concept_id not in (1585846,1585847), 'Unknown',value_source_value) as value_source_value,questionnaire_response_id FROM raw.observation WHERE observation_source_concept_id in (SELECT concept_id FROM raw.concept WHERE REGEXP_CONTAINS(concept_code,'(?i)sex_at_birth'))   UNION ALL SELECT observation_id,person_id,observation_concept_id,observation_type_concept_id,value_as_number,IF(value_source_concept_id not in (8515,38003598,8527,1585609,38003574,1585615,1585628,1585344,8516,1585614,1585627,1585343,1586155,1586146,1586142,1586151,1586152,1586143,1585523),'Other Race',value_as_string) as value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,IF(value_source_concept_id not in (8515,38003598,8527,1585609,38003574,1585615,1585628,1585344,8516,1585614,1585627,1585343,1586155,1586146,1586142,1586151,1586152,1586143,1585523), 'Other Race',observation_source_value) as observation_source_value,IF(value_as_string == 'Multi-Racial',2000000,value_source_concept_id not in (8515,38003598,8527,1585609,38003574,1585615,1585628,1585344,8516,1585614,1585627,1585343,1586155,1586146,1586142,1586151,1586152,1586143,1585523),8522,observation_source_concept_id)) as observation_source_concept_id,unit_source_value,qualifier_source_value,value_source_concept_id,IF(value_source_concept_id not in (8515,38003598,8527,1585609,38003574,1585615,1585628,1585344,8516,1585614,1585627,1585343,1586155,1586146,1586142,1586151,1586152,1586143,1585523), 'Other Race',value_source_value) as value_source_value,questionnaire_response_id FROM raw.observation WHERE observation_source_concept_id in (SELECT concept_id FROM raw.concept WHERE REGEXP_CONTAINS(concept_code,'(?i)race'))   UNION ALL SELECT observation_id,person_id,observation_concept_id,observation_type_concept_id,value_as_number,IF(value_source_concept_id not in (1585948,1585946,1585945,1585941),'Unknown',value_as_string) as value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,IF(value_source_concept_id not in (1585948,1585946,1585945,1585941), 'Unknown',observation_source_value) as observation_source_value,IF(value_source_concept_id not in (1585948,1585946,1585945,1585941),0,observation_source_concept_id) as observation_source_concept_id,unit_source_value,qualifier_source_value,value_source_concept_id,IF(value_source_concept_id not in (1585948,1585946,1585945,1585941), 'Unknown',value_source_value) as value_source_value,questionnaire_response_id FROM raw.observation WHERE observation_source_concept_id in (SELECT concept_id FROM raw.concept WHERE REGEXP_CONTAINS(concept_code,'(?i)education'))   UNION ALL SELECT observation_id,person_id,observation_concept_id,observation_type_concept_id,value_as_number,IF(value_source_concept_id not in (1585956,1585955,1585953),'Unknown',value_as_string) as value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,IF(value_source_concept_id not in (1585956,1585955,1585953), 'Unknown',observation_source_value) as observation_source_value,IF(value_source_concept_id not in (1585956,1585955,1585953),0,observation_source_concept_id) as observation_source_concept_id,unit_source_value,qualifier_source_value,value_source_concept_id,IF(value_source_concept_id not in (1585956,1585955,1585953), 'Unknown',value_source_value) as value_source_value,questionnaire_response_id FROM raw.observation WHERE observation_source_concept_id in (SELECT concept_id FROM raw.concept WHERE REGEXP_CONTAINS(concept_code,'(?i)employment')) ) GROUP BY observation_id,person_id,observation_concept_id,observation_type_concept_id,value_as_number,value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,observation_source_value,observation_source_concept_id,unit_source_value,qualifier_source_value,value_source_concept_id,value_source_value,questionnaire_response_id) a INNER JOIN (
                        SELECT x.person_id,
                DATE_DIFF( CAST(x.value_as_string AS DATE), CAST(__targetTable.observation_date AS DATE), DAY) as observation_date
            ,
                DATE_DIFF( CAST(x.value_as_string AS DATE), CAST(__targetTable.observation_datetime AS DATE), DAY) as observation_datetime
             
                        FROM raw.observation x INNER JOIN 
                            raw.observation __targetTable

                        ON __targetTable.person_id = x.person_id 
                        WHERE x.observation_source_value = 'ExtraConsent_TodaysDate'
                         AND x.observation_id = __targetTable.observation_id
                        
                    ) p ON p.person_id = a.person_id  UNION ALL SELECT observation_id,person_id,observation_concept_id,observation_type_concept_id,value_as_number,value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,observation_source_value,observation_source_concept_id,unit_source_value,qualifier_source_value,value_source_concept_id,value_source_value,questionnaire_response_id ,observation_date,observation_datetime FROM ( 
                    
                        SELECT CAST (DATE_DIFF(CAST(x.value_as_string AS DATE),CAST(y.value_as_string AS DATE),DAY) as STRING) as value_as_string, x.person_id, 
                DATE_DIFF( CAST(x.value_as_string AS DATE), CAST(observation_date AS DATE), DAY) as observation_date
            ,
                DATE_DIFF( CAST(x.value_as_string AS DATE), CAST(observation_datetime AS DATE), DAY) as observation_datetime
              ,provider_id,observation_concept_id,value_as_number,observation_id,unit_source_value,unit_concept_id,value_source_value,qualifier_concept_id,observation_source_value,observation_type_concept_id,observation_source_concept_id,qualifier_source_value,value_as_concept_id,value_source_concept_id,questionnaire_response_id,visit_occurrence_id
                        FROM raw.observation x INNER JOIN (
                            SELECT MAX(value_as_string) as value_as_string, person_id
                            FROM raw.observation
                            WHERE observation_source_value = 'ExtraConsent_TodaysDate'
                            
                            GROUP BY person_id
                        ) y ON x.person_id = y.person_id 
                        
                        WHERE observation_source_value in (
                            
                            SELECT concept_code from raw.concept 
                            WHERE REGEXP_CONTAINS(concept_code,'(DATE|Date|date)') IS TRUE
                            
                        )
                         
                     ) ) WHERE REGEXP_CONTAINS(observation_source_value,'Text|TextBox|_City|_Zip|WordAddress|OrganTransplatDescription_|OutsideTravel6Months_|PersonalMedicalhistory_AdditionalDiagnosis|circulatory_HowOldWereYouHypertension|GeneralConsent_OtherComments|PPIFeedback_|DiagnosisHistory_WhichConditions|OtherCancer_|PostPMBFeedback_|_HowOldWereYou|_AreYouAPatientAllOfUsPartner|PIIName_|SocialSecurity|PIIAddress_|Address_PII|_Phone|_Email|Signature|SanDiegoBloodBank|AZSArizonaSpecific') IS FALSE AND person_id in (781579)
