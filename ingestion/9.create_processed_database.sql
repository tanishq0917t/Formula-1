-- Databricks notebook source
create database if not exists f1_processed
location "/mnt/tanishqformula1dl/processed"

-- COMMAND ----------

desc database f1_processed