# Accounts of the training branch

```sql accounts
select account_id, holder_name, city, balance, balance_band
from dev.demo_accounts_clean
order by account_id
```

{% table data="accounts" /%}
