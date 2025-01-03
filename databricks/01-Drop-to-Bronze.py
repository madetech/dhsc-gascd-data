# Databricks notebook source
from pyspark.sql.functions import explode, flatten, col
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType
import os
import pandas as pd

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

drop_restricted_path = "abfss://restricted@dapalphastdrop" + environment + ".dfs.core.windows.net/"
bronze_restricted_path = "abfss://restricted@dapalphastbronze" + environment + ".dfs.core.windows.net/"

# COMMAND ----------

datetime = "20241224000004"

# COMMAND ----------

json_data = spark.read.format("json").load(drop_restricted_path + "Capacity_Tracker/Generic/" + datetime)

# COMMAND ----------

df_items = json_data.select(explode("items").alias('items'))

# Flatten the DataFrame
flattened_df = df_items.select(flatten_schema(df_items.schema))

# COMMAND ----------

#vaccine table
vaccine_df = flattened_df.select("items_cqcId",explode("items_vaccinations").alias('vaccinations'))
flat_vaccine_df = vaccine_df.select(flatten_schema(vaccine_df.schema))

# COMMAND ----------

#vacancy table
vacancies_df = flattened_df.select("items_cqcId",explode("items_vacancies").alias('vacancies'))
flat_vacancies_df = vacancies_df.select(flatten_schema(vacancies_df.schema))

# COMMAND ----------

flattened_df.createOrReplaceTempView("flattened_df")
main = spark.sql(
    f'''
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
    items_workforce_nursesEmployed AS workforce_nurses_employed,
    '{datetime}' as load_date_time
    FROM flattened_df;
    '''
)

# COMMAND ----------

flat_vaccine_df.createOrReplaceTempView('flat_vaccine_df')
vaccinations = spark.sql(
    f'''
    SELECT
    items_cqcId AS cqc_id,
    vaccinations_questionGroup AS question_group,
    vaccinations_questionId AS question_id,
    vaccinations_questionText AS question_text,
    vaccinations_response AS response,
    '{datetime}' as load_date_time
    FROM flat_vaccine_df
    '''
)

# COMMAND ----------

flat_vacancies_df.createOrReplaceTempView('flat_vacancies_df')
vacancies = spark.sql(
    f'''
    SELECT
    items_cqcId AS cqc_id,
    vacancies_bedType AS bed_type,
    vacancies_closed AS closed,
    vacancies_closureReason AS closure_reason,
    vacancies_reserved AS reserved,
    vacancies_spare AS spare,
    vacancies_total AS total,
    vacancies_used AS used,
    '{datetime}' as load_date_time
    FROM flat_vacancies_df
    '''
)

# COMMAND ----------

# Write DataFrame to processed container
main.coalesce(1).write.format("parquet").mode("overwrite").option("header", "true").save(bronze_restricted_path + "Capacity_Tracker/Generic/main/" + datetime + ".parquet")
vaccinations.coalesce(1).write.format("parquet").mode("overwrite").option("header", "true").save(bronze_restricted_path + "Capacity_Tracker/Generic/vaccinations/" + datetime + ".parquet")
vacancies.coalesce(1).write.format("parquet").mode("overwrite").option("header", "true").save(bronze_restricted_path + "Capacity_Tracker/Generic/vacancies/" + datetime + ".parquet")
