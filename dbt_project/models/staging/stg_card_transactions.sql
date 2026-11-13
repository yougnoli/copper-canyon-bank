-- Every card transaction: the history, then the daily files (when there are any: before the first delivery there is
-- no table of daily files yet).
{% set daily = load_relation(source('loaded', 'card_transactions')) %}

select
    tx_id,
    card_id,
    merchant_name,
    merchant_category,
    amount,
    currency,
    tx_ts,
    tx_type,
    original_tx_id,
    channel,
    status
from {{ source('history', 'card_transactions_hist') }}
{% if daily is not none %}
union all

select
    tx_id,
    card_id,
    merchant_name,
    merchant_category,
    cast(amount as numeric(12, 2)) as amount,
    currency,
    cast(tx_ts as timestamptz) as tx_ts,
    tx_type,
    original_tx_id,
    channel,
    status
from {{ daily }}
{% endif %}
