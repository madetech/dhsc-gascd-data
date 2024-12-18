# Databricks notebook source
from pyspark.sql.functions import explode, flatten, col
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType
import os

# COMMAND ----------

# Function to recursively flatten the schema
def flatten_schema(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, StructType):
            fields += flatten_schema(dtype, prefix=name)
        else:
            fields.append(col(name).alias(name.replace('.', '_')))
    return fields

# COMMAND ----------

# Get environment variable
environment = os.getenv("ENV")

raw_container_path = "abfss://raw@dapalphadatastlake" + environment + ".dfs.core.windows.net/"
processed_container_path = "abfss://processed@dapalphadatastlake" + environment + ".dfs.core.windows.net/"
reporting_container_path = "abfss://reporting@dapalphadatastlake" + environment + ".dfs.core.windows.net/"

# COMMAND ----------

data = spark.read.format("json").load(raw_container_path + "Capacity_Tracker/Generic/*").collect()

# COMMAND ----------

df = spark.createDataFrame(data)
df_items = df.select(explode("items").alias("items"))

# Flatten the DataFrame
flattened_df = df_items.select(flatten_schema(df_items.schema))
flattened_df.createOrReplaceTempView("flattened_df")

# COMMAND ----------

ct_main = spark.sql(
    '''
    SELECT
    items_cqcId AS cqc_id,
    items_locationName AS location_name,
    items_cqcType AS cqc_type,
    items_geography_icbName AS icb_name,
    items_geography_icbOdsCode AS icb_ods_code,
    items_geography_laCode AS la_code,
    items_geography_laName AS la_name,
    items_geography_laRegionName AS la_region_name,
    items_geography_laSnacCode AS la_snac_code,
    items_geography_lrfName AS lrf_name,
    items_geography_nhseRegionName AS nhse_region_name,
    items_geography_subIcbName AS sub_icb_name,
    items_geography_subIcbOdsCode AS sub_icb_ods_code,
    items_acceptingOutOfHoursAdmissions AS accepting_out_of_hours_admissions,
    items_admissionStatus AS admission_status,
    items_ascSupport_hasIdentifiedPcnLead AS has_identified_pcn_lead,
    items_ascSupport_hasStaffInfectionTraining AS has_staff_infection_training,
    items_ascSupport_hasWeeklyCheckin AS has_weekly_checkin,
    items_ascSupport_isVisitorsInsideCareHome AS is_visitors_inside_care_home,
    items_ascSupport_isVisitsOffCareHomePremises AS is_visits_off_care_home_premises,
    items_confirmedSave AS confirmed_save,
    items_internationalRecruitment_intlVisaCountCare AS intl_visa_count_care,
    items_internationalRecruitment_intlVisaCountNurses AS intl_visa_count_nurses,
    items_internationalRecruitment_intlVisaIsLicensed AS intl_visa_is_licensed,
    items_isAcute AS is_acute,
    items_isCareHome AS is_care_home,
    items_isCommunity AS is_community,
    items_isHospice AS is_hospice,
    items_isSubstanceMisuse AS is_substance_misuse,
    items_odsCode AS ods_code,
    items_providerOdsCode AS provider_ods_code,
    items_reportInclusion AS report_inclusion,
    items_totalResidentCount AS total_resident_count,
    items_vacanciesLastUpdated AS vacancies_last_updated,
    items_workforce_agencyCareWorkersEmployed AS workforce_agency_care_workers_employed,
    items_workforce_agencyNonCareWorkersEmployed AS workforce_agency_non_care_workers_employed,
    items_workforce_agencyNursesEmployed AS workforce_agency_nurses_employed,
    items_workforce_careWorkersAbsent AS workforce_care_workers_absent,
    items_workforce_careWorkersAbsentCovid AS workforce_care_workers_absent_covid,
    items_workforce_careWorkersEmployed AS workforce_care_workers_employed,
    items_workforce_daysAbsence AS workforce_days_absence,
    items_workforce_hoursAbsence AS workforce_hours_absence,
    items_workforce_hoursAgency AS workforce_hours_agency,
    items_workforce_hoursOvertime AS workforce_hours_overtime,
    items_workforce_hoursPaid AS workforce_hours_paid,
    items_workforce_nonCareWorkersAbsent AS workforce_non_care_workers_absent,
    items_workforce_nonCareWorkersAbsentCovid AS workforce_non_care_workers_absent_covid,
    items_workforce_nonCareWorkersEmployed AS workforce_non_care_workers_employed,
    items_workforce_nursesAbsent AS workforce_nurses_absent,
    items_workforce_nursesAbsentCovid AS workforce_nurses_absent_covid,
    items_workforce_nursesEmployed AS workforce_nurses_employed
FROM flattened_df;
'''
)
ct_main.createOrReplaceTempView("ct_main")

# COMMAND ----------

# Write DataFrame to processed container
ct_main.coalesce(1).write.format("parquet").mode("overwrite").option("header", "true").save(processed_container_path + "Capacity_Tracker/Generic/ct_main.parquet")

# COMMAND ----------

# SQL query to select specific columns from the DataFrame
provider_metrics = spark.sql(
    '''
    SELECT
        cqc_id,
        location_name,
        la_code,
        la_name,
        la_region_name,
        workforce_hours_agency / (workforce_hours_paid + workforce_hours_agency) AS percentage_workforce_hours_agency,
        workforce_hours_overtime / workforce_hours_paid AS percentage_direct_workforce_hours_overtime
    FROM ct_main
    '''
)

# Unpivot the DataFrame to have cqcm location, la_code, la_name, la_region, metric, value columns
provider_metrics_unpivoted = provider_metrics.selectExpr(
    "cqc_id",
    "location_name",
    "la_code",
    "la_name",
    "la_region_name",
    "stack(2, 'Percentage of total hours worked that are agency', percentage_workforce_hours_agency, 'Percentage of total hours worked by direct employees that are overtime', percentage_direct_workforce_hours_overtime) as (metric, value)"
)
provider_metrics_unpivoted.createOrReplaceTempView("provider_metrics_unpivoted")

# COMMAND ----------

# SQL query to select specific columns from the DataFrame
la_metrics = spark.sql(
    '''
    SELECT
        la_code,
        la_name,
        la_region_name,
        SUM(workforce_hours_agency) / (SUM(workforce_hours_paid) + SUM(workforce_hours_agency)) AS percentage_workforce_hours_agency,
        SUM(workforce_hours_overtime) / SUM(workforce_hours_paid) AS percentage_direct_workforce_hours_overtime
    FROM ct_main
    GROUP BY
        la_code,
        la_name,
        la_region_name
    '''
)

la_metrics_unpivoted = la_metrics.selectExpr(
    "la_code",
    "la_name",
    "la_region_name",
    "stack(2, 'Percentage of total hours worked that are agency', percentage_workforce_hours_agency, 'Percentage of total hours worked by direct employees that are overtime', percentage_direct_workforce_hours_overtime) as (metric, value)"
)
la_metrics_unpivoted.createOrReplaceTempView("la_metrics_unpivoted")

# COMMAND ----------

# SQL query to select specific columns from the DataFrame
region_metrics = spark.sql(
    '''
    SELECT
        la_region_name,
        SUM(workforce_hours_agency) / (SUM(workforce_hours_paid) + SUM(workforce_hours_agency)) AS percentage_workforce_hours_agency,
        SUM(workforce_hours_overtime) / SUM(workforce_hours_paid) AS percentage_direct_workforce_hours_overtime
    FROM ct_main
    GROUP BY
        la_region_name
    '''
)

region_metrics_unpivoted = region_metrics.selectExpr(
    "la_region_name",
    "stack(2, 'Percentage of total hours worked that are agency', percentage_workforce_hours_agency, 'Percentage of total hours worked by direct employees that are overtime', percentage_direct_workforce_hours_overtime) as (metric, value)"
)
region_metrics_unpivoted.createOrReplaceTempView("region_metrics_unpivoted")

# COMMAND ----------

# Union the regional, LA, and provider metrics
ct_metrics = spark.sql(
    '''
    SELECT
        'care_provider' as location_level,
        cqc_id as location_id,
        location_name as location_name,
        la_code as parent_location_id,
        la_name as parent_location_name,
        la_region_name as grandparent_location_name,
        metric,
        value
    FROM provider_metrics_unpivoted
    UNION ALL
    SELECT
        'local_authority' as location_level,
        la_code as location_id,
        la_name as location_name,
        null as parent_location_id,
        la_region_name as parent_location_name,
        null as grandparent_location_name,
        metric,
        value
    FROM la_metrics_unpivoted
        UNION ALL
    SELECT
        'region' as location_level,
        null as location_id,
        la_region_name as location_name,
        null as parent_location_id,
        null as parent_location_name,
        null as grandparent_location_name,
        metric,
        value
    FROM region_metrics_unpivoted
    '''
)

# COMMAND ----------

# Write DataFrame to reporting container
ct_metrics.coalesce(1).write.format("parquet").mode("overwrite").option("header", "true").save(reporting_container_path + "Capacity_Tracker/Generic/ct_metrics.parquet")
