from pyspark.sql import SparkSession
import sys,datetime,json
from  pyspark.sql.functions import input_file_name

day_key=None
mo_key=None 
year_key=None
hour_key='99'
app_name=None
variableDict={}

def main():      
    global variableDict,app_name,day_key,hour_key,mo_key,year_key
    sparkSession = None
    strStartTime = None
    results = []
    try:
        dtStartTime = datetime.datetime.now()
        strStartTime = dtStartTime.strftime("%d-%m-%Y %H:%M:%S")    
        argumentDict={}
        # Print command line arguments
        for arg in sys.argv[1:]:
            split = arg.split("=",1)
            argumentDict[split[0]] = split[1]
            print arg
            
        # Check app_name
        app_name = getVariable(argumentDict, "app_name")
        # Check workflow_name
        workflow_name = getVariable(argumentDict, "workflow_name")
        # Check mo_key
        mo_key = getVariable(argumentDict, "mo_key")
        print('------------------------------------------------------------------------------------------------------------------------------------')
        print('mo_key: ' + mo_key)
                       
        # Init spark session 
        sparkSession = SparkSession.builder.appName(app_name).config("spark.sql.crossJoin.enabled", "true").enableHiveSupport().getOrCreate();
        
        df = sparkSession.sql("""  
                                 SELECT  	
                                    CAST(DAY_KEY AS STRING) 						AS day_key, 	
                                    CAST(SERVICE_NBR AS STRING) 					AS service_nbr, 	
                                    CAST(ACCT_SRVC_INSTANCE_KEY AS STRING) 			AS acct_srvc_instance_key, 	
                                    CAST(FACE_ID AS STRING) 						AS face_id, 	
                                    CAST(USE_ZALO AS STRING) 						AS use_zalo, 	
                                    CAST(USE_VIBER AS STRING) 						AS use_viber, 	
                                    CAST(USE_FACEBOOK AS STRING) 					AS use_facebook, 	
                                    CAST(USE_INSTAGRAM AS STRING) 					AS use_instagram, 	
                                    CAST(USE_E_WALLET AS STRING) 					AS use_e_wallet, 	
                                    CAST(USE_UTILITY_APPS AS STRING) 				AS use_utility_apps, 	
                                    CAST(SUB_RANK AS STRING) 						AS sub_rank, 	
                                    CAST(USE_BANK AS STRING) 						AS use_bank, 	
                                    CAST(BANK_NAME AS STRING)						AS bank_name, 	
                                    CAST(USE_SOCIAL_INSURANCE AS STRING) 			AS use_social_insurance, 	
                                    CAST(USE_LIFE_INSURANCE AS STRING) 				AS use_life_insurance, 	
                                    CAST(FAVOURITE_CHANEL_1 AS STRING) 				AS favourite_chanel_1, 	
                                    CAST(FAVOURITE_CHANEL_2 AS STRING) 				AS favourite_chanel_2,
                                    CAST(FAVOURITE_CHANEL_3 AS STRING) 				AS favourite_chanel_3,
                                    CAST(BLOCK_DATA AS STRING) 						AS block_data,
                                    CAST(PROD_LINE_KEY AS STRING) 					AS prod_line_key,
                                    CAST(SUB_AGE AS STRING) 						AS sub_age,
                                    CAST(ARPU_GROUP AS STRING) 						AS arpu_group,
                                    CAST(LFCYCL_STAT_CD AS STRING) 					AS lfcycl_stat_cd,
                                    CAST(OFFER_SBRP AS STRING) 						AS offer_sbrp,
                                    CAST(PROD_SBRP AS STRING) 						AS prod_sbrp,
                                    CAST(RVN_AMT AS STRING) 						AS rvn_amt,
                                    CAST(VOICE_RVN_AMT AS STRING) 					AS voice_rvn_amt,
                                    CAST(SMS_RVN_AMT AS STRING) 					AS sms_rvn_amt,
                                    CAST(DATA_RVN_AMT AS STRING)					AS data_rvn_amt,
                                    CAST(VAS_RVN_AMT AS STRING)			 			AS vas_rvn_amt,
                                    CAST(RMNG_ISD_RVN_AMT AS STRING)				AS rmng_isd_rvn_amt,
                                    CAST(OTH_RVN_AMT AS STRING) 					AS oth_rvn_amt,
                                    CAST(VOICE_USG AS STRING) 						AS voice_usg,
                                    CAST(VOICE_ONNET_USG AS STRING)					AS voice_onnet_usg,
                                    CAST(VOICE_OFFNET_USG AS STRING)				AS voice_offnet_usg,
                                    CAST(VOICE_ISD_USG AS STRING)					AS voice_isd_usg,
                                    CAST(SMS_EVT_CNT AS STRING)			 			AS sms_evt_cnt,
                                    CAST(SMS_ONNET_EVT_CNT AS STRING) 				AS sms_onnet_evt_cnt,
                                    CAST(SMS_OFFNET_EVT_CNT AS STRING) 				AS sms_offnet_evt_cnt,
                                    CAST(SMS_ISD_EVT_CNT AS STRING) 				AS sms_isd_evt_cnt,
                                    CAST(TOT_FLUX AS STRING) 						AS tot_flux,
                                    CAST(LAST_ACTIVITY_DT AS STRING) 				AS last_activity_dt,
                                    CAST(USAGE_DAYS_CNT AS STRING) 					AS usage_days_cnt,
                                    CAST(AVG_VOICE_EVT_CNT AS STRING) 				AS avg_voice_evt_cnt,
                                    CAST(AVG_SMS_EVT_CNT AS STRING) 				AS avg_sms_evt_cnt,
                                    CAST(AVG_TOT_FLUX AS STRING) 					AS avg_tot_flux,
                                    CAST(VOICE_RMNG_ISD_EVT_CNT AS STRING) 			AS voice_rmng_isd_evt_cnt,
                                    CAST(SMS_RMNG_ISD_EVT_CNT AS STRING) 			AS sms_rmng_isd_evt_cnt,
                                    CAST(DATA_RMNG_ISD_EVT_CNT AS STRING) 			AS data_rmng_isd_evt_cnt,
                                    CAST(VOICE_ISD_OG_EVT_CNT AS STRING) 			AS voice_isd_og_evt_cnt,
                                    CAST(SMS_ISD_OG_EVT_CNT AS STRING)				AS sms_isd_og_evt_cnt,
                                    CAST(USE_MOBIFONE_PAY AS STRING) 				AS use_mobifone_pay,
                                    CAST(USE_MOBIFONE_FIBER AS STRING) 				AS use_mobifone_fiber,
                                    CAST(USE_MOBIFONE_MONEY AS STRING) 				AS use_mobifone_money,
                                    CAST(USE_VAS AS STRING) 						AS use_vas,
                                    CAST(DROP_CALLS AS STRING) 						AS drop_calls,
                                    CAST(HNDST_TYP_CD AS STRING) 					AS hndst_typ_cd,
                                    CAST(HNDST_BRND_CD AS STRING) 					AS hndst_brnd_cd,
                                    CAST(HNDST_MDL_CD AS STRING) 					AS hndst_mdl_cd,
                                    CAST(LOYALTY_RANK AS STRING) 					AS loyalty_rank,
                                    CAST(CHURN_IND_T2 AS STRING) 					AS churn_ind_t2,
                                    CAST(RVN_FORECAST_T2 AS STRING) 				AS rvn_forecast_t2,
                                    CAST(VOICE_CC_EVT_CNT AS STRING) 				AS voice_cc_evt_cnt,
                                    CAST(COMPLAINT_SOCIAL_EVT_CNT AS STRING) 		AS complaint_social_evt_cnt,
                                    CAST(CUST_STFN_SCORE AS STRING) 				AS cust_stfn_score,
                                    CAST(NPS_IND AS STRING) 						AS nps_ind,
                                    CAST(ICE_NETWORK_IND AS STRING) 				AS ice_network_ind,
                                    CAST(ICE_SERVICE_IND AS STRING) 				AS ice_service_ind,
                                    CAST(ICE_CUSTOMER_CARE_IND AS STRING) 			AS ice_customer_care_ind,
                                    CAST(TOPIC_TOP AS STRING) 						AS topic_top,
                                    CAST(WEB_TOP AS STRING) 						AS web_top,
                                    CAST(APP_TOP AS STRING) 						AS app_top,
                                    CAST(DATA07_TOT_FLUX AS STRING) 				AS data07_tot_flux,
                                    CAST(DATA724_TOT_FLUX AS STRING) 				AS data724_tot_flux,
                                    CAST(REPLACE(LAST_STAY_CELL_KEY,'"','') AS STRING) 				AS last_stay_cell_key,
                                    CAST(REPLACE(LAST_ACTIVITY_CELL_KEY,'"','') AS STRING) 			AS last_activity_cell_key,
                                    CAST(ABROAD_FREQUENCY AS STRING) 				AS abroad_frequency,
                                    CAST(ABROAD_TIMES AS STRING) 					AS abroad_times,
                                    CAST(VN_FREQUENCY AS STRING) 					AS vn_frequency,
                                    CAST(MOST_BEHAVIOR_MOVE AS STRING)		 		AS most_behavior_move,
                                    CAST(MOST_PLACE_IN_WORKING_TIME AS STRING) 		AS most_place_in_working_time,
                                    CAST(MOST_PLACE_OUT_WORKING_TIME AS STRING) 	AS most_place_out_working_time,
                                    CAST(MOST_PLACE_IN_WEEK AS STRING) 				AS most_place_in_week,
                                    CAST(MOST_PLACE_IN_WEEKEND AS STRING) 			AS most_place_in_weekend,
                                    CAST(MOST_PLACE AS STRING) 						AS most_place,
                                    CAST(REPLACE(BM_RESIDENCE,'"','') AS STRING) 					AS bm_residence,
                                    CAST(REPLACE(BM_WORKPLACE,'"','') AS STRING) 					AS bm_workplace,
                                    CAST(REPLACE(BM_OCCUPATION,'"','') AS STRING) 					AS bm_occupation,
                                    CAST(LAST_INTERACT_DT AS STRING) 				AS last_interact_dt,
                                    CAST(LAST_INTERACT_CHANEL AS STRING) 			AS last_interact_chanel,
                                    CAST(LAST_INTERACT_TOPIC AS STRING) 			AS last_interact_topic,
                                    CAST(MOST_USED_SOCIAL_NTWK AS STRING) 			AS most_used_social_ntwk,
                                    CAST(MOST_DAY_SOCIAL_NTWK AS STRING) 			AS most_day_social_ntwk,
                                    CAST(MOST_TIME_SOCIAL_NTWK AS STRING) 			AS most_time_social_ntwk,
                                    CAST(TOP_VOICE_OG AS STRING) 					AS top_voice_og,
                                    CAST(TOP_VOICE_IC AS STRING) 					AS top_voice_ic,
                                    CAST(lower(CHURN_IND) AS STRING) 						AS churn_ind,
                                    CAST(CREDIT_SCORING AS STRING) 					AS credit_scoring,
                                    CAST(CUSTOMER_SEGMENT AS STRING) 				AS customer_segment,
                                    CAST(CUSTOMER_HOBBY AS STRING) 					AS customer_hobby,
                                    CAST(SUBSCRIPTION_FEE_EMAIL AS STRING) 			AS subscription_fee_email,
                                    CAST(USE_PORTAL AS STRING) 						AS use_portal,
                                    CAST(USE_CHANNEL_CHAT AS STRING) 				AS use_channel_chat,
                                    CAST(SUB_ID AS STRING) 							AS sub_id,
                                    CAST(COR_ID AS STRING) 							AS cor_id,
                                    CAST(LOYAL_MARK AS STRING) 						AS loyal_mark,
                                    CAST(TOPIC_VOICE_CNT AS STRING) 				AS topic_voice_cnt,
                                    CAST(RCHRG_PAYMENT_TYPE_FAVOURITE AS STRING) 	AS rchrg_payment_type_favourite,
                                    CAST(RCHRG_PAYMENT_CHANEL_FAVOURITE AS STRING) 	AS rchrg_payment_chanel_favourite,
                                    CAST(ECASH_RECHARGE_PAYMENT AS STRING) 			AS ecash_recharge_payment,
                                    CAST(BILL_SHOCK AS STRING) 						AS bill_shock,
                                    CAST(CHURN_SERVICE AS STRING) 					AS churn_service,
                                    CAST(REGISTER_CHANNEL_LAST AS STRING) 			AS register_channel_last, 
                                    CAST(DROP_CALL_48H AS STRING) 					AS drop_call_48h,
                                    CAST(COMPLAIN_PROCESSING_CNT AS STRING) 		AS complain_processing_cnt,
                                    CAST(CC_CMPLNT_CNT AS STRING) 					AS cc_cmplnt_cnt,
                                    CAST(PACK_IND AS STRING) 						AS pack_ind,
                                    CAST(PAYMENT_IND AS STRING) 					AS payment_ind,
                                    CAST(BSNS_CLUSTER_KEY AS STRING) 				AS bsns_cluster_key,
                                    CAST(BSNS_CLUSTER_CD AS STRING) 				AS bsns_cluster_cd,
                                    CAST(LAST_UPDT_DT AS STRING) 					AS last_updt_dt,
                                    CAST(LAST_UPDT_BY AS STRING) 					AS last_updt_by,
                                    cast(usage_data_ind as string)         			as usage_data_ind,
                                    cast(rvn_amt_ind as string)           			as rvn_amt_ind,
                                    cast(use_service_ind as string)           		as use_service_ind,
                                    cast(ntwk_access_ind as string)         		as ntwk_access_ind,
                                    cast(quality_ntwk_ind as string)         		as quality_ntwk_ind,
                                    cast(activation_dt as string)         			as activation_dt,
                                    cast(lst_offer_suggest as string)         			as lst_offer_suggest,
                                    CAST(MO_KEY AS STRING) 							AS mo_key 
                                FROM MBF_BIGDATA.ADMD_ACCOUNT_EXTEND_INFO WHERE MO_KEY IN('""" + mo_key +"""')""")                                                                                 
      
        #df.repartition(210).write.option("sep",",").option("header","true").option("emptyValue", "").mode("overwrite").csv("/DATALAKE_TLS_TEST/SOLR/ACCOUNT_EXTEND_INFO")
        df.repartition(50).write.option("sep",",").option("header","true").option("emptyValue", "").mode("overwrite").csv("/DATAWH/TMP/SOLR/ACCOUNT_EXTEND_INFO")
        
    except Exception as err:
        raise
    finally:      
        #Spark stop  
        if(sparkSession != None):
            sparkSession.stop()           
     
    
def getVariable(argumentDict, name):  
    try:
        result = argumentDict[name]
        #print(result)
        return result
    except Exception as e:
        print(e)    
        #print("ERR------------------------------")
        #print(name + " argument is not exists")
        raise Exception(name + " argument is not exists") 
            
if __name__ == '__main__':
    main()