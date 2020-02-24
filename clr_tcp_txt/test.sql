declare @s nvarchar(max) = '';
--set @s = dbo.tcp___send_cache('192.168.11.205',9999,'POL_PAWN','select TOP 10 * from mobile.pol_pawn'); --90s 
--set @s = dbo.tcp___send_cache('192.168.11.205',9999,'POL_PROCESS','select TOP 2 * from mobile.pol_online_process'); --140 
--set @s = dbo.tcp___send_cache_indexs('192.168.11.205',6969,'POL_PROCESS','select top 2 * from mobile.pol_online_process order by id desc','*','*','*','*','int_pol_pawn_id');
--set @s = dbo.tcp___send_cache_indexs('192.168.11.205',6969,'POL_PAWN','select TOP 2 * from mobile.pol_pawn','*','*','*','*','');
--set @s = dbo.tcp___send_cache_indexs('192.168.11.205',9999,'POL_PAWN_DETAIL','select TOP 2 * from mobile.pol_pawn_detail','*','*','*','*','');

--select * from dbo.json___column_type('select TOP 2 * from mobile.pol_pawn');
select * from json___query('select TOP 2 * from mobile.pol_pawn order by ids desc');
--select * from json___table_key_value(N'{"id":1,"step_id":-1,"int_queued":-1,"int_status":1,"asset_type_id":1,"channel_id":-1,"area_id":-1,"city_id":-1,"district_id":-1,"int_days":90,"lng_money":100000000,"customer_id":1,"str_asset_type_name":"Ô tô","str_channel_name":"","str_city_name":"","str_description":"Mới 99%","str_district_name":"","str_product_year":"2010","str_trademark":"Mercedec","str_url":"","int_created_date":20160121,"int_created_time":105741,"user_created_id":-1,"cus_created_id":-1,"shop_id":-1,"caller_shop_id":-1,"caller_online_id":1206,"group_id":44,"int_group_status":4,"bit_car_in_bank":-1,"int_priority_id":2,"int_sms":-1,"str_group_ids_reveiced":"44","int_trans_to_shop_date":20160121,"int_trans_to_shop_time":105741,"int_reference_type":-1,"str_reference_affilate_id":"","user_cancel_id":-1,"int_cancel_date":-1,"int_cancel_time":-1,"int_set_caller_online_date":20200203,"int_set_caller_online_time":105300,"int_pawn_id_pos":-1,"int_customer_id_pos":-1,"int_loan_money_pos":-1,"int_loan_days_pos":-1,"int_create_date_pos":-1,"int_from_date_pos":-1,"str_codeno_pos":"","str_category_code_pos":"","int_status_pos":-2,"int_loan_money_org_pos":-1,"int_is_f88_cus_pos":-1,"str_pos_asset_code_pos":"","int_order_priority":3,"is2step":null}')
--where LEFT(Id, 1) != '$'

print @s;