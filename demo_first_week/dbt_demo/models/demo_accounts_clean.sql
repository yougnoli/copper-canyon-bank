-- The training week's clean table: the same accounts, with the balance as money (two decimals, dlt widened it) and a band
-- for each balance. A table: dbt builds it again from scratch every time it runs.
select
    account_id,
    holder_name,
    city,
    opened_on,
    cast(balance as numeric(12, 2)) as balance,
    case
        when balance < 1000 then 'LOW'
        when balance >= 5000 then 'HIGH'
        else 'MEDIUM'
    end as balance_band
from {{ source('loaded', 'demo_accounts') }}
