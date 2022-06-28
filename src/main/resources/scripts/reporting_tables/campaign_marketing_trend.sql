insert into `robust-index-352517.reporting_tables.campaign_marketing_trend`
with device_metrics as (
with temp as (select a.campaign_id, u.user_device_type, count(user_device_type) as share
 from `robust-index-352517.reporting_tables.campaign_agenda_results` a
JOIN `robust-index-352517.source_tables.hubspot_email_open_event` b
ON a.campaign_id = b.campaign_id
join `robust-index-352517.source_tables.user_details` u
ON b.user_id = u.user_id
where a.campaign_open_positive = true
group by a.campaign_id, u.user_device_type)
select * from temp pivot (sum(share) for user_device_type in ('tablet', 'mobile', 'laptop'))),
state_metrics as (with temp as (select a.campaign_id, replace(u.user_state, ' ', '_') as user_state, count(user_state) as state_share
 from `robust-index-352517.reporting_tables.campaign_agenda_results` a
JOIN `robust-index-352517.source_tables.hubspot_email_open_event` b
ON a.campaign_id = b.campaign_id
join `robust-index-352517.source_tables.user_details` u
ON b.user_id = u.user_id
where a.campaign_open_positive = true
group by a.campaign_id, u.user_state)
select * from temp pivot (sum(state_share) for user_state in ('Illinois','California','Georgia','Arizona','Florida','West_Virginia','Texas','Massachusetts','Wisconsin','District_of_Columbia','Kentucky','Missouri','Mississippi','Alabama','Ohio','Michigan','Oklahoma','Virginia','New_York','Washington','Colorado','Pennsylvania','Indiana','North_Carolina','Minnesota','Tennessee','New_Jersey','Idaho','Nevada','Louisiana','New_Mexico','Maryland','South_Carolina','Oregon','Connecticut','Alaska','Iowa','Montana','Rhode_Island','Utah','Arkansas','Kansas','Nebraska','Hawaii','North_Dakota','New_Hampshire','Maine','South_Dakota')))
select a.course_campaign_name,
a.campaign_agenda,
a.campaign_category,
a.digital_marketing_team,
a.marketing_product,
b.mobile, b.tablet, b.laptop, c.* from `robust-index-352517.reporting_tables.campaign_agenda_results` a
join device_metrics b
on a.campaign_id = b.campaign_id
join state_metrics c
on a.campaign_id = c.campaign_id;
