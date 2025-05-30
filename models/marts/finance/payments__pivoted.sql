{% set payment_methods = ['bank_transfer','coupon','gift_card','credit_card'] %}

with payments as (
    select * from {{ ref('stg_payments') }}
)

select 
order_id,
{% for payment_method in payment_methods %}

    sum(case when payment_method = '{{payment_method}}' then amount else 0 end) as {{payment_method}}_amount,

{% endfor %}

from payments
where status = 'success'
group by 1