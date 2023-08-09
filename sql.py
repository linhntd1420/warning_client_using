extract_sql = """WITH ps_list AS (
	SELECT
		cd.ID cart_detail_id,
		CAST ( PUBLIC.convert_timezone (cd.created ) AS DATE ) AS change_time,
		cd.app_id,
		cd.gift_id,
		cd.gift_detail_id,
		cd.brand_id,
CASE
		when CAST ( PUBLIC.convert_timezone ( cd.created ) AS DATE ) <= '2023-06-01' then 385000
		when cast ( PUBLIC.convert_timezone ( cd.created ) AS DATE ) <= '2023-06-01' and lower(gd.title) like '%le saigonnais%' then 405000
		else 385000
		END as MONEY,

CASE
		WHEN gc.active = 2 THEN
		'Đã sử dụng'
		WHEN gc.active = 1
		AND ( DATE_ADD ( 'day', 15, CAST ( CONVERT_TIMEZONE ( gc.created ) AS DATE ) ) < CURRENT_DATE ) THEN
			'Hủy booking (ko bảo lưu code cho KH)'
		 ELSE
			'Chưa sử dụng'
		END AS code_status,
case
	when gc.active = 2 and cd.app_id = 754 then CAST(convert_timezone(used) AS DATE)
	when ps.using_date is not null then cast(ps.using_date as date)
	else null
	end as using_time

FROM
	urbox.cart_detail cd
	LEFT JOIN urbox.gift_detail gd ON cd.gift_detail_id = gd.ID
	LEFT JOIN urbox.gift_code gc on gc.cart_detail_id = cd.ID
	left join (select distinct stt, code_ub,code,using_date, brand_id
								from ub_rawdata.cs_premium
								where len(code) > 0 and using_status not in ('Hủy booking (ko bảo lưu code cho KH)'))  ps
								on gc.code = ps.code

WHERE
	cd.status = 2
	AND cd.pay_status = 2
	AND gd.TYPE <> 16
	AND cd.brand_id IN ( SELECT DISTINCT brand_id FROM ub_rawdata.cs_premium_brand )
	)
SELECT
	sum(case when code_status = 'Đã sử dụng' then money else 0 end) as total_money
FROM
	ps_list
WHERE
	app_id in ({APP_IDS}) and code_status = 'Đã sử dụng' and using_time >= '{USING_TIME}'"""

