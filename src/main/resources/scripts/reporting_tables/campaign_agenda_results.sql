insert into `robust-index-352517.reporting_tables.campaign_agenda_results`
WITH event_sent_metrics AS
(
         SELECT   Concat(campaign_id,'-', course_campaign_name)                  AS campaign_results_key,
                  Cast((Sum(event_status) / Count(event_status)*100) AS INTEGER) AS campaign_actual_sent_percent,
                  Sum(event_status)                                              AS campaign_actual_sent
         FROM     `robust-index-352517.source_tables.hubspot_email_sent_event`
         GROUP BY campaign_id,
                  course_campaign_name),
event_open_metrics AS
(
         SELECT   concat(campaign_id,'-', course_campaign_name)                  AS campaign_results_key,
                  cast((sum(event_status) / count(event_status)*100) AS integer) AS campaign_actual_open_percent,
                  sum(event_status)                                              AS campaign_actual_open
         FROM     `robust-index-352517.source_tables.hubspot_email_open_event`
         GROUP BY campaign_id,
                  course_campaign_name),
event_click_metrics AS
(
         SELECT   concat(campaign_id,'-', course_campaign_name)                  AS campaign_results_key,
                  cast((sum(event_status) / count(event_status)*100) AS integer) AS campaign_actual_click_percent,
                  sum(event_status)                                              AS campaign_actual_click
         FROM     `robust-index-352517.source_tables.hubspot_email_open_event`
         GROUP BY campaign_id,
                  course_campaign_name),
event_unsubscribe_metrics AS
(
         SELECT   concat(campaign_id,'-', course_campaign_name)                  AS campaign_results_key,
                  cast((sum(event_status) / count(event_status)*100) AS integer) AS campaign_actual_unsubscribe_percent,
                  sum(event_status)                                              AS campaign_actual_unsubscribe
         FROM     `robust-index-352517.source_tables.hubspot_email_sent_event`
         GROUP BY campaign_id,
                  course_campaign_name)
SELECT esm.campaign_results_key,
       campaign_id,
       course_campaign_name,
       course_campaign_start_date,
       course_campaign_end_date,
       campaign_agenda,
       campaign_category,
       digital_marketing_team,
       marketing_product,
       campaign_agenda_sent,
       campaign_actual_sent_percent,
       campaign_actual_sent ,
       IF (campaign_actual_sent_percent >= campaign_agenda_sent, true, false) as campaign_sent_positive,
       campaign_agenda_open,
       eom.campaign_actual_open_percent,
       eom.campaign_actual_open,
       IF (eom.campaign_actual_open_percent >= campaign_agenda_open, true, false) as campaign_open_positive,
       campaign_agenda_click,
       ecm.campaign_actual_click_percent,
       ecm.campaign_actual_click,
       IF (ecm.campaign_actual_click_percent > campaign_agenda_click, true, false) as campaign_click_positive,
       campaign_agenda_unsubscribe,
       eum.campaign_actual_unsubscribe_percent,
       eum.campaign_actual_unsubscribe,
       IF (eum.campaign_actual_unsubscribe_percent < campaign_agenda_unsubscribe, true, false) as campaign_unsubscribe_positive

FROM   event_sent_metrics esm
JOIN   `robust-index-352517.source_tables.campaign_details`
ON     esm.campaign_results_key=concat(campaign_id,'-', course_campaign_name)
JOIN event_open_metrics eom
ON esm.campaign_results_key=eom.campaign_results_key
JOIN event_click_metrics ecm
ON ecm.campaign_results_key = esm.campaign_results_key
JOIN event_unsubscribe_metrics eum
ON eum.campaign_results_key = eum.campaign_results_key;