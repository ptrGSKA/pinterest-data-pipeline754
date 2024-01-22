# Pinterest Data Pipeline

Designed as an image sharing and social media platform, Pinterest serves as a valuable tool for discovering and saving information, particularly focused on "ideas" like recipes, home decor, fashion, motivation, and inspiration. By utilizing images, animated GIFs, and videos, users can create pinboards to curate and explore content on the internet.

## Table of Contents

1. [Introduction](#1-introduction)
2. [Description](#2-description)

    2.1 [Pipeline architechture](#21-pipeline-architechture)  

3. [Tools used](#tools-used)
4. [Installation](#5-installation)
5. [File structure](#file-structure)
6. [Licence](#licence)

## 1. Introduction

The objective of this project is to construct a pair of data pipelines that will facilitate the transfer of information from Pinterest by leveraging AWS cloud computing services and Databricks for data processing.

Batch processing is primarily utilized in situations where there is a need to handle substantial data quantities or when data sources consist of legacy systems incapable of providing data in real-time streams. Under the batch processing model, data is accumulated over time and then fed into an analytics system.

Real-time analytics results heavily rely on stream processing. Through the creation of data streams, the generated data is immediately fed into analytics tools, allowing for near-instantaneous analytics outcomes using platforms like Spark Streaming. In this streaming model, the data is processed piece-by-piece, ensuring real-time processing.

## 2. Description

The primary goal of this project is to establish an end-to-end AWS-hosted data pipeline based on Pinterest's experiment processing pipeline. This pipeline is developed using a Lambda architecture, wherein the batch data is ingested via AWS API Gateway and AWS MSK, and then stored in an AWS S3 bucket. Subsequently, the batch data is retrieved from the S3 bucket and processed using Apache Spark in Databricks. To handle streaming data in near real-time, Spark Structured Streaming in Databricks is employed to read the data from AWS Kinesis and store it in Databricks Delta Tables for long-term retention.

### 2.1 Pipeline architechture  

![Cloud Pinterest Pipeline](images/cloud-pinterest-pipeline.webp)

## Tools used

- [Amazon API Gateway](https://aws.amazon.com/api-gateway/) - Amazon API Gateway is a fully managed service that makes it easy for developers to create, publish, maintain, monitor, and secure APIs at any scale. APIs act as the "front door" for applications to access data, business logic, or functionality from your backend services.

- [Amazon Simple Storage Service (Amazon S3)](https://aws.amazon.com/s3/) - It's an object storage service provides unparalleled scalability, data availability, security, and performance. It caters to customers of various sizes and industries, enabling them to securely store and safeguard any volume of data for a wide range of purposes. Whether it's data lakes, cloud-native applications, or mobile apps.

- [Amazon Kinesis](https://aws.amazon.com/kinesis/) - Amazon Kinesis is a fully managed service that efficiently handles the processing and analysis of streaming data at any scale. By utilizing Kinesis, we can  seamlessly ingest real-time data, including video, audio, application logs, website clickstreams, and IoT telemetry data. This data can then be utilized for various applications such as machine learning (ML), analytics, and more.

- [Amazon Elastic Compute Cloud (Amazon EC2)](https://aws.amazon.com/ec2/) - EC2 is a cloud computing platform to run virtual computers and application in the cloud. It provides on-demand, scalable computing capacity to  develop and deploy applications.

## 5. Installation

Follow the steps to start the pipeline process:

## File structure

The project's structure:

```text
.
├── creds.yaml
├── README.md
└── user_posting_emulation.py
```

## Licence
