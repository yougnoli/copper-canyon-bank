-- Purchases for each day (UTC), for the CFO's daily check against the processor's report.
select
    cast(t.tx_ts at time zone 'UTC' as date) as card_date,
    count(*) as purchases,
    sum(t.amount) as purchases_amount
from {{ ref('stg_card_transactions') }} as t
inner join {{ ref('stg_cards') }} as c on t.card_id = c.card_id
where t.tx_type = 'PURCHASE'
group by 1
