# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    return input_df.withColumn("ingestion_date",current_timestamp())