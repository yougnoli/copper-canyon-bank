-- The card list of core banking.
select
    card_id,
    account_id,
    card_type,
    issued_on,
    reissue_seq
from {{ source('loaded', 'core_cards') }}
