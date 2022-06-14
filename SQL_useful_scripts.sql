
----------------------------------------------------------------------1) Finding new customers
--  Input: customer-month pairs (optional: with revenue)
--- Output: count of new customers in each month. Table with columns: month, total customers, new customers (revenue by customer type)


------- customer spending by month
CREATE TABLE customer_spending_in_mnth WITH (appendonly=true, compresstype=zlib, compresslevel=5) as
select d.customer_id, cash_month, round(sum(total_transaction_amt - total_transaction_undo_amt),0) trans_amt
from some_schema.aggregate_sales_data d
where 1=1 and cash_month_dt >= '2021-01-01' and cash_month_dt <= '2022-03-01'	
group by 1,2,3
distributed by (customer_id)

-------------------------------------------------- flag new customers
---explain
CREATE TABLE new_customer_flags WITH (appendonly=true, compresstype=zlib, compresslevel=5) as
with t as (
			select customer_id, cash_month_dt, 															-- CURRENT visit
					lag(cash_month_dt) over(partition by customer_id order by cash_month_dt) prev_month -- previous visit
					, min(cash_month_dt) over(order by cash_month_dt) min_data							-- earliest date IN a dataset
			from (	
					select  distinct customer_id, cash_month_dt
					from customer_spending_in_mnth
					) q
		)	
select
	  customer_id
	, cash_month_dt
	, prev_month
	, case 
		   when prev_month is null  then 1 
		   else 0 end as new_flg
from t 
order by customer_id, cash_month_dt
distributed by (customer_id)


------ also, if we need to find reactivated customers: that came back after X months,3 for exampe - we can use this
/* when prev_month is null and cash_month_dt > date_trunc('month',min_data) + interval '3 month'  then 1  --- exp: came in october 2020, but dataset FROM january 2021.
		   when date_trunc('month', prev_month::date)::date < date_trunc('month', cash_month_dt::date - interval '3 month')::date  then 1
		   else 0 end as reactivated_flg

*/

-------------------------------------------------- table with new/reactivate customers by month
select 
		  d.cash_month_dt
		, count(distinct case when new_flg = 1 then d.customer_id else null end) clt_new
		, count(distinct case when new_flg = 0 then d.customer_id else null end) clt_current

		, round(sum(case when new_flg = 1 then trans_amt else null end),0) amt_new
		, round(sum(case when new_flg = 0 then trans_amt else null end),0) amt_current

		, count(distinct d.customer_id) clt
		, round(sum(trans_amt),0) trans_amt
from customer_spending_in_mnth d 
inner join new_customer_flags b on b.customer_id = d.customer_id and d.cash_month_dt = b.cash_month_dt
group by 1
order by 1







----------------------------------------------------------------------2) Segments/buckets by spending

----- saels data
create table customer_sales_data WITH (appendonly=true, compresstype=zlib, compresslevel=5) as
(select d.* from schema_sales.transactions d
where d.cash_dttm between current_date - interval '14 month' and current_date
)
DISTRIBUTED BY (transaction_id);



-- Buckets by bill
  select 
     	  date_trunc('month',cash_dttm)
     	, case  
     			when transaction_amt >= 10000 then 1000
     			when transaction_amt >= 5000 then 500
     			when transaction_amt >= 2000 then 200
     			when transaction_amt >= 1800 then 200     			
     			when transaction_amt >= 1500 then 150
     			when transaction_amt >= 1300 then 130
     			when transaction_amt >= 1000 then 100
     			when transaction_amt >= 900 then 90
     			when transaction_amt >= 850 then 85
     			when transaction_amt >= 800 then 80
     			when transaction_amt >= 750 then 75
     			when transaction_amt >= 700 then 70
     			when transaction_amt >= 650 then 65
     			when transaction_amt >= 600 then 50
		     	when transaction_amt >= 550 then 55
		     	when transaction_amt >= 500 then 50
		     	when transaction_amt >= 450 then 45
		     	when transaction_amt >= 400 then 40
		     	when transaction_amt >= 350 then 35
     	else 0 end as bill_bucket
	 	, count(distinct customer_id) clients
	 	, count(distinct transaction_id) tranx
	    , round(sum(transaction_amt),0) transaction_amt 
from  customer_sales_data
where 1=1 and loyalty_level_id <> 0
group by 1,2


-- Buckets by revenue per customer


create table lenta_segments WITH (appendonly=true, compresstype=zlib, compresslevel=5) as
with t1 as ------- IN subselect we GROUP revenue BY customer AND month
			(select 	date_trunc('month',cash_dttm) cash_month 
					, 	customer_id
	 			    , sum(transaction_amt)amt
	 			    , count(distinct transaction_id)cnt_txns 
 			  	from  customer_sales_data --andep. 
   			  	where 1=1 and loyalty_level_id <> 0
 				group by 1,2)
select cash_month, customer_id,  ---- bucketing
		case 
			 when amt>= 15000  then 150
			 when amt>= 10000  then 100
			 when amt>= 8500  then 85
			 when amt>= 7000  then 70
			 when amt>= 6500  then 65
			 when amt>= 6000  then 60
			 when amt>= 5500  then 55
			 when amt>= 5000  then 50
			 when amt>= 4500  then 45
			 when amt>= 4000  then 40
			 when amt>= 3500  then 35
			 when amt>= 3000  then 30
			 when amt>= 2500  then 25
			 when amt>= 2000  then 20
			 when amt>= 1500   then 15
			 else 0 end as segm2_txns,
 		amt,
 		cnt_txns
from t1
distributed by (customer_id);




---------------------------------------------------------------------- 3) Rolling LTV
----- visit and revenue log
drop TABLE IF exists visit_log_1;
create table visit_log_1 WITH (appendonly=true, compresstype=zlib, compresslevel=5) AS
select 	date_trunc('month',cash_dttm) cash_month 
					, 	customer_id
	 			    , sum(transaction_amt)amt
	 			    , count(distinct transaction_id)cnt_txns 
from  customer_sales_data
group by 1,2
distributed by (customer_id)


--- Rolling LTV (12 months)
drop table cummulative_ltv
create table cummulative_ltv WITH (appendonly=true, compresstype=zlib, compresslevel=5) as
select
	a.customer_id, 
	mnth_visits, 
	sum(vl_amt_all) over (partition by a.customer_id order by 
						  mnth_visits range between interval '0 month' preceding and interval '12 month' following) ltv_sum
from visit_log_1 a  
order by mnth_visits 
distributed by (customer_id)


----- avrrage LTV per month
---- also, when provided with brands - we can add them to the script for more detalisation
explain
select 
		mnth_visits
	,	round(avg(ltv_sum),0) LTV
from cummulative_ltv
--where date_trunc('month', mnth_visits)  <= '2020-06-01'
group by 1,2










