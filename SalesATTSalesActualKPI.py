#This module for Sales AT&T Tablet Data KPI #############

from pyspark.sql import SparkSession,SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import split
import sys,os
import logging
from datetime import datetime
import collections
from pyspark.sql.types import StructType,StringType,IntegerType,StructField
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit
from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as sf


ATTSalesActualInp = sys.argv[1]
ATTSalesActualKPIOp = sys.argv[2]
FileTime = sys.argv[3]

spark = SparkSession.builder.\
        appName("SalesAT&TTabletDataKPI").getOrCreate()
        
#########################################################################################################
#                                 Read the source files                                              #
#########################################################################################################
        
dfATTSalesActualInp  = spark.read.parquet(ATTSalesActualInp)
dfATTSalesActualInp.registerTempTable("attsalesactual")

'''
Report Date: DATE NOT NULL
Sto re Numb er : INTEGER NOT NULL (FK)
Source Emp loy ee Id: INTEGE R NOT NULL (FK)
KPI Name: V ARCHAR(50 ) NOT NULL (FK)
Company Cd: INTEGER NOT NULL (FK)
KPI V al ue: INTEGE R NOT NULL
'''


dfATTTabletData = spark.sql("select a.storenumber,'ATTTabletData' as kpiname,"
                + "sum(a.actualvalue) as kpivalue,first_value(a.businessdate) as report_date,first_value(a.companycd) as companycd "
                + "from attsalesactual a "
                + "where a.kpi = 'Tablet Pstpd GA Cnt' "
                + "group by a.storenumber ")
                
dfSmartCheckCnt = spark.sql("select a.storenumber,'SmartCheckCnt' as kpiname,"
                + "sum(a.actualvalue) as kpivalue,first_value(a.businessdate) as report_date,first_value(a.companycd) as companycd "
                + "from attsalesactual a "
                + "where a.kpi = 'Total SmartChk Session Cnt' "
                + "group by a.storenumber ")

dfOpportunities = spark.sql("select a.storenumber,'Opportunities' as kpiname,"
                + "sum(a.actualvalue) as kpivalue,first_value(a.businessdate) as report_date,first_value(a.companycd) as companycd "
                + "from attsalesactual a "
                + "where a.kpi = 'Opp Cnt' "
                + "group by a.storenumber ")

dfPremiumVideo = spark.sql("select a.storenumber,'PremiumVideo' as kpiname,"
                + "sum(a.actualvalue) as kpivalue,first_value(a.businessdate) as report_date,first_value(a.companycd) as companycd "
                + "from attsalesactual a "
                + "where a.kpi IN ('Adv TV Total Net Submit Sales Cnt','Premium Video Net Submit Sales Cnt') "
                + "group by a.storenumber ")

dfATTBroadband = spark.sql("select a.storenumber,'ATTBroadband' as kpiname,"
                + "sum(a.actualvalue) as kpivalue,first_value(a.businessdate) as report_date,first_value(a.companycd) as companycd "
                + "from attsalesactual a "
                + "where a.kpi = 'Broadband Internet Sales' "
                + "group by a.storenumber ")

dfATTSurveyCnt = spark.sql("select a.storenumber,'ATTSurveyCnt' as kpiname,"
                + "sum(a.actualvalue) as kpivalue,first_value(a.businessdate) as report_date,first_value(a.companycd) as companycd "
                + "from attsalesactual a "
                + "where a.kpi = 'ACE - WTR Completed Survey Cnt (trans)' "
                + "group by a.storenumber ")

dfATTInsuredEligOppCnt = spark.sql("select a.storenumber,'ATTInsuredEligOppCnt' as kpiname,"
                + "sum(a.actualvalue) as kpivalue,first_value(a.businessdate) as report_date,first_value(a.companycd) as companycd "
                + "from attsalesactual a "
                + "where a.kpi = 'Insured Elig Opp Cnt' "
                + "group by a.storenumber ")

dfATTInsuredFeatAddCnt = spark.sql("select a.storenumber,'ATTInsuredFeatAddCnt' as kpiname,"
                + "sum(a.actualvalue) as kpivalue,first_value(a.businessdate) as report_date,first_value(a.companycd) as companycd "
                + "from attsalesactual a "
                + "where a.kpi = 'Insured Feat Add Cnt' "
                + "group by a.storenumber ")

dfATTMPPFeatCnt = spark.sql("select a.storenumber,'ATTMPPFeatCnt' as kpiname,"
                + "sum(a.actualvalue) as kpivalue,first_value(a.businessdate) as report_date,first_value(a.companycd) as companycd "
                + "from attsalesactual a "
                + "where a.kpi = 'MPP Feat Cnt' "
                + "group by a.storenumber ")
                
dfATTMDPPFeatCnt = spark.sql("select a.storenumber,'ATTMPPFeatCnt' as kpiname,"
                + "sum(a.actualvalue) as kpivalue,first_value(a.businessdate) as report_date,first_value(a.companycd) as companycd "
                + "from attsalesactual a "
                + "where a.kpi = 'MDPP Feat Cnt' "
                + "group by a.storenumber ")

dfATTWTRScore = spark.sql("select a.storenumber,'ATTWTRScore' as kpiname,"
                + "avg(a.actualvalue) as kpivalue,first_value(a.businessdate) as report_date,first_value(a.companycd) as companycd "
                + "from attsalesactual a "
                + "where a.kpi = 'WTR' "
                + "group by a.storenumber ")
                
dfATTWaitTimeScore = spark.sql("select a.storenumber,'ATTWaitTimeScore' as kpiname,"
                + "sum(a.actualvalue) as kpivalue,first_value(a.businessdate) as report_date,first_value(a.companycd) as companycd "
                + "from attsalesactual a "
                + "where a.kpi = 'ACE - Wait Time < 5 Mins' "
                + "group by a.storenumber ")



#"where a.kpi = 'Tablet Pstpd GA Cnt' "#
#a.businessdate as reportdate,
todayyear = datetime.now().strftime('%Y')
todaymonth = datetime.now().strftime('%m')


dfATTTabletData.coalesce(1).select("*"). \
write.parquet(ATTSalesActualKPIOp + '/' + todayyear + '/' + todaymonth + '/' + 'SalesATTTabletKPI' + FileTime)

dfSmartCheckCnt.coalesce(1).select("*"). \
write.parquet(ATTSalesActualKPIOp + '/' + todayyear + '/' + todaymonth + '/' + 'SalesSmartCheckCntKPI' + FileTime);

dfOpportunities.coalesce(1).select("*"). \
write.parquet(ATTSalesActualKPIOp + '/' + todayyear + '/' + todaymonth + '/' + 'SalesATTTabletKPI' + FileTime);

dfPremiumVideo.coalesce(1).select("*"). \
write.parquet(ATTSalesActualKPIOp + '/' + todayyear + '/' + todaymonth + '/' + 'SalesATTTabletKPI' + FileTime);

dfATTBroadband.coalesce(1).select("*"). \
write.parquet(ATTSalesActualKPIOp + '/' + todayyear + '/' + todaymonth + '/' + 'SalesATTTabletKPI' + FileTime);

dfATTSurveyCnt.coalesce(1).select("*"). \
write.parquet(ATTSalesActualKPIOp + '/' + todayyear + '/' + todaymonth + '/' + 'SalesATTTabletKPI' + FileTime);

dfATTInsuredEligOppCnt.coalesce(1).select("*"). \
write.parquet(ATTSalesActualKPIOp + '/' + todayyear + '/' + todaymonth + '/' + 'SalesATTInsuredEligOppCntKPI' + FileTime);

dfATTInsuredFeatAddCnt.coalesce(1).select("*"). \
write.parquet(ATTSalesActualKPIOp + '/' + todayyear + '/' + todaymonth + '/' + 'SalesATTInsuredFeatAddCntKPI' + FileTime);

dfATTMPPFeatCnt.coalesce(1).select("*"). \
write.parquet(ATTSalesActualKPIOp + '/' + todayyear + '/' + todaymonth + '/' + 'SalesATTMPPFeatCntKPI' + FileTime);

dfATTMDPPFeatCnt.coalesce(1).select("*"). \
write.parquet(ATTSalesActualKPIOp + '/' + todayyear + '/' + todaymonth + '/' + 'SalesATTMDPPFeatCntKPI' + FileTime);

dfATTWTRScore.coalesce(1).select("*"). \
write.parquet(ATTSalesActualKPIOp + '/' + todayyear + '/' + todaymonth + '/' + 'SalesATTWTRScoreKPI' + FileTime);

dfATTWaitTimeScore.coalesce(1).select("*"). \
write.parquet(ATTSalesActualKPIOp + '/' + todayyear + '/' + todaymonth + '/' + 'SalesATTWaitTimeScoreKPI' + FileTime);


                
spark.stop()


                   