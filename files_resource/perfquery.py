#****************************************************************************
# (C) Cloudera, Inc. 2020-2022
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import sys, random, os, json

# EXPECTATIONS ON ALL COLUMNS

def main():

    ## CDE PROPERTIES
    dbname = "CDE_GPU_DEMO"
    username = "pauldefusco"

    print("\nRunning as Username: ", username)
    print("\nUsing DB Name: ", dbname)

    #---------------------------------------------------
    #               CREATE SPARK SESSION WITH ICEBERG
    #---------------------------------------------------
    #spark = SparkSession.builder.appName('INGEST').config("spark.kubernetes.access.hadoopFileSystems", data_lake_name).getOrCreate()

    spark = SparkSession\
        .builder\
        .appName("BANK TRANSACTIONS DATA QUALITY")\
        .getOrCreate()

    SQL = """
            select  *
            from (select avg(transaction_amount) B1_LP
                        ,count(email) B1_CNT
                        ,count(distinct address) B1_CNTD
                  from {0}.BANKING_TRANSACTIONS_{1}
                  where transaction_amount between 0 and 1000
                    and (longitude between 10 and 40
                         or latitude between 0 and 0+10) B1,
                 (select avg(accounts) B2_LP
                        ,count(name) B2_CNT
                        ,count(distinct aba_routing) B2_CNTD
                  from {0}.BANKING_TRANSACTIONS_{1}
                  where age between 60 and 70
                    and (total_transactions between 20 and 20+100
                      or age between 10 and 10+50
                      or credit_cards between 4 and 4+20)) B2,
                 (select avg(total_transactions) B3_LP
                        ,count(bank_country) B3_CNT
                        ,count(distinct swift11) B3_CNTD
                  from {0}.BANKING_TRANSACTIONS_{1}
                  where credit_cards between 11 and 15
                    and (total_transactions between 66 and 66+10
                      or age between 10 and 10+100
                      or latitude between 4 and 4+20)) B3,
             LIMIT 1000;
    """.format(dbname, username)

    df = spark.sql(SQL)

    print("\nSHOW TOP TEN ROWS")
    df.show(10)

    print("\nPRINT DF SCHEMA")
    df.printSchema()

if __name__ == "__main__":
    main()
