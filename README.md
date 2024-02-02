# Pinterest Data Pipeline

<div align="center">
	<table>
		<tr>
			<td><code><img width="40" src="https://user-images.githubusercontent.com/25181517/192107858-fe19f043-c502-4009-8c47-476fc89718ad.png" alt="REST" title="REST"/></code></td>
			<td><code><img width="40" src="https://user-images.githubusercontent.com/25181517/192107004-2d2fff80-d207-4916-8a3e-130fee5ee495.png" alt="kafka" title="kafka"/></code></td>
			<td><code><img width="40" src="https://user-images.githubusercontent.com/25181517/192108372-f71d70ac-7ae6-4c0d-8395-51d8870c2ef0.png" alt="Git" title="Git"/></code></td>
			<td><code><img width="40" src="https://user-images.githubusercontent.com/25181517/192108374-8da61ba1-99ec-41d7-80b8-fb2f7c0a4948.png" alt="GitHub" title="GitHub"/></code></td>
			<td><code><img width="40" src="https://user-images.githubusercontent.com/25181517/192108891-d86b6220-e232-423a-bf5f-90903e6887c3.png" alt="Visual Studio Code" title="Visual Studio Code"/></code></td>
			<td><code><img width="40" src="https://user-images.githubusercontent.com/25181517/183423507-c056a6f9-1ba8-4312-a350-19bcbc5a8697.png" alt="Python" title="Python"/></code></td>
			<td><code><img width="40" src="https://user-images.githubusercontent.com/25181517/183896132-54262f2e-6d98-41e3-8888-e40ab5a17326.png" alt="AWS" title="AWS"/></code></td>	
			<td><code><img width="40" src="https://user-images.githubusercontent.com/25181517/184357834-eba1eee1-6074-4b9c-8ed3-5373868096cc.png" alt="Apache Spark" title="Apache Spark"/></code></td>
			<td><code><img width="40" src="https://user-images.githubusercontent.com/25181517/197845567-86a09ca9-d96f-42c4-9ab1-8bce95ab000d.png" alt="Databricks" title="Databricks"/></code></td>
		</tr>
	</table>
</div>

Designed as an image sharing and social media platform, Pinterest serves as a valuable tool for discovering and saving information, particularly focused on "ideas" like recipes, home decor, fashion, motivation, and inspiration. By utilizing images, animated GIFs, and videos, users can create pinboards to curate and explore content on the internet.

## Table of Contents

1. [Introduction](#1-introduction)
2. [Description](#2-description)  
 2.1 [Pipeline architechture](#21-pipeline-architechture)  
 2.2 [Project walkthrough](#22-project-walkthrough)
3. [Tools used](#3-tools-used)
4. [Installation](#4-installation)  
 4.1 [Set up AWS RDS](#41-set-up-aws-rds)  
 4.2 [Create MSK Cluster](#42-create-msk-cluster)  
 4.3 [Configuring EC2 Kafka client](#43-configuring-ec2-kafka-client)  
 4.4 [Connect MSK cluster to S3 bucket](#44-connect-msk-cluster-to-s3-bucket)  
 4.5 [Configuring API gateway](#45-configuring-api-gateway)  
 4.6 [Batch processing Databricks](#46-batch-processing-databricks)  
 4.7 [AWS MWAA](#47-aws-mwaa)  
 4.8 [AWS Kinesis](#48-aws-kinesis)  
5. [File structure](#5-file-structure)
6. [Licence](#6-licence)

## 1. Introduction

The objective of this project is to construct a pair of data pipelines that will facilitate the transfer of information from Pinterest by leveraging AWS cloud computing services and Databricks for data processing.

Batch processing is primarily utilized in situations where there is a need to handle substantial data quantities or when data sources consist of legacy systems incapable of providing data in real-time streams. Under the batch processing model, data is accumulated over time and then fed into an analytics system.

Real-time analytics results heavily rely on stream processing. Through the creation of data streams, the generated data is immediately fed into analytics tools, allowing for near-instantaneous analytics outcomes using platforms like Spark Streaming. In this streaming model, the data is processed piece-by-piece, ensuring real-time processing.

## 2. Description

The primary goal of this project is to establish an end-to-end AWS-hosted data pipeline based on Pinterest's experiment processing pipeline. This pipeline is developed using a Lambda architecture, wherein the batch data is ingested via AWS API Gateway and AWS MSK, and then stored in an AWS S3 bucket. Subsequently, the batch data is retrieved from the S3 bucket and processed using Apache Spark in Databricks. To handle streaming data in near real-time, Spark Structured Streaming in Databricks is employed to read the data from AWS Kinesis and store it in Databricks Delta Tables for long-term retention.

### 2.1 Pipeline architechture  

![Cloud Pinterest Pipeline](images/cloud-pinterest-pipeline.webp)

### 2.2 Project walkthrough

To kickstart the project, I took the first steps of creating a Github repository and setting up an AWS account.
The project requires __AWS EC2__ services where a virtual machine (VM) is running to serve as the client machine in the pinterest project pipeline.
To proceed with accessing the __EC2__ instance, it requires a .pem file. This file format is commonly used to store cryptographic keys, including SSH certificates and their corresponding private keys. By selecting the instance from the __EC2__ instance console and navigating to the details section, the key-pair-name identifier was retrieved, which will serve as the filename for the `.pem` file.

After successfully establishing a secure __SSH__ connection to the VM, the next course of action involves installing the essential packages and softwares (_Java, Kafka and IAM authentication package_) on the instance to establish a connection with an existing __MSK__ cluster running on AWS.

__Apache Kafka__ will be utilized in the project to manage data streaming, which must be installed on the VM. Once Kafka is successfully installed, the __IAM MSK authentication package__ is also necessary to establish a connection with MSK, which utilizes __IAM authentication__.

By accessing the Roles section in the __IAM__ console, the instance role was obtained and subsequently modified with the `unique user-ec2-access` role ARN. This modification grants the instance the ability to connect to the MSK cluster.

With the modification of the `client.properties` file, the configuration of the Kafka client is done, facilitating effective communication between the instance and the cluster.

To ensure the accessibility of __MSK IAM__ libraries to the Kafka client, it is necessary to set up the `CLASSPATH` environment variable in the .bashrc file, to store the locationof the jar file, prior to creating any topic.
Retrieving the necessary information about the cluster from AWS MSK management console, comprising the `Bootstrap servers string` and the `Plaintext Apache Zookeeper connection string` so that the necessary topics can be created. There are 3 topics that will be used throught the project, the `<>.pin`, `<>.geo` and `<>.user`.

To ensure the preservation of data after it has been read from the topics, an __AWS S3__ bucket and __VPC__ endpoint are being established and configured. The __Confluent Amazon S3 Connector__ is then downloaded and transferred to the __S3__ bucket via the __EC2__ instance. This connector serves as a sink, responsible for extracting data from the Kafka topics and storing it in the S3 bucket. In the MSK Console, a new plugin is created under custom plugins, which is located within the S3 bucket. Subsequently, a new connector is created and configured with MSK Connect, allowing data to be streamed through the cluster and saved in the S3 bucket using this plugin-connector pair.

An __API__ has been developed using the __Kafka REST proxy__ method to ensure the seamless reception of data. This API is seamlessly connected to the EC2 instance, where a web proxy actively monitors incoming data.
To facilitate API integration with the Kafka REST Proxy on the EC2 instance, the __Confluent package__ is installed and configured. The IAM authentication process is performed to ensure secure access. Once the setup is successfully completed, the system is ready to receive data.

Emulation of the data involves the retrieval of random data from an __AWS RDS__ and its subsequent transmission to the `invoke URL` of the API,which was set up during the API's creation. Once the build process is completed successfully, the system becomes capable of seamlessly consuming data from the topics,where an S3 storage repository receives the data automatically.

Upon sending data to the API and passing it through the MSK cluster, it is automatically stored in an S3 bucket, organized into separate directories for each topic. The execution of data preparation and processing is accomplished through __Apache Spark__ on __Databricks__. To facilitate analysis, effective communication between the two platforms is necessary. This entails creating a new user with full access policy to the S3 bucket, generating the required _security credentials_, and downloading them.

The process on Databricks involves uploading the credentials and creating a table to complete the task. In a fresh python notebook, the essential modules are imported and the integration with S3 is executed. The necessary information includes the _encoded security credentials_ with the `access key` and `secret access key`, the `AWS bucket name`, and the `mounting point`.  
The listing of the mounted bucket `display(dbutils.fs.ls("/mnt/mount_name/.."))` reveals the content of the S3 bucket on Databricks, confirming its preparedness for the next step.

#### 2.2.1 Data cleaning

After integrating, it is imperative to clean the data contained in each bucket.

`df_pin` table cleaning:

- Replacing entries with no relevant data.
- Cleaning up the follower count by replacing the `k` and `M` with the relevant numeric quivalent.
- Converting each numeric type column to int.
- Cleaning up the save location, leaving only the path.
- Renaming the index column.
- Reordering the table.

`df_geo` table cleaning:

- Creating a new array column `coordinates` with the latitude and longitude.
- Dropping the latitude and longitude columns.
- Converting the timestamp column into timestamp type and cleaning it up.
- Reordering the columns.

`df_user` table cleaning:

- Creating a new column `user_name` containing both the first_name and last_name.
- Dropping the first_name and last_name columns.
- Converting the timestamp column into timestamp type and cleaning it up.
- Reordering the columns.

#### 2.2.2 Data querying

The following question are being answered:

- __What are the most popular Pinterest category people post to based on their country?__  
![Batch query-1](images/batch_q1.png)

- __How many posts each category had between 2018 and 2022?__  
![Batch query-2](images/batch_q2.png)

- __Who are the most followed users in each country?__  
![Batch query-3](images/batch_q3a.png)
![Batch query-3](images/batch_q3b.png)

- __What are the most popular categories by age group?__  
![Batch query-4](images/batch_q4.png)

- __What is the median follower count for users by age group?__  
![Batch query-5](images/batch_q5.png)

- __What are the number of users joined each year?__  
![Batch query-6](images/batch_q6.png)

- __What us the median follower count of users based on their joining year?__  
![Batch query-7](images/batch_q7.png)

- __What are the median follower count of users based on their joining year and age group?__  
![Batch query-8](images/batch_q8.png)

Once the connection between Databricks and MWAA is established, a requirements.txt file is generated and uploaded to S3. With the necessary configurations in place, everything is now set for running the dag. A `DAG` has been created and uploaded to the `dags` directory on the running `MWAA` environment. When navigating through the `Airflow` UI, the item is initially categorized under the paused section. Once it is changed to active, a daily run will be triggered and executed automatically.

The final phase of this project entailed the implementation of streaming data using AWS Kinesis. To begin streaming data with Kinesis, the initial step involves creating streams. The The subsequent streams are responsible for the
data transport: `streaming-<>-pin`, `streaming-<>-geo` and `streaming-<>-user`. The streaming process is currently utilizing the API that was created earlier, therefore it requires modification in order to effectively manage streams. An additional IAM policy has been created and linked to a role, enabling the API to have unrestricted access to Kinesis.  
The following modifications were made to the API:

- New resource has been created and configured `streams` and a method to list streams `ListStreams`

- New child resource has been created under streams `{stream-name}` with three additional methods `DescribeStream`, `CreateStream` and `DeleteStream` respontible for describing, creating and deleting a stream.

- Two additional child resources have been created under _{stream-name}_, `record` and `records` that are responsible for a single or multiple record reading and transformation to send data to Kinesis.

The data posting simulation, which interacts with the API streaming endpoint, is managed by the file __user_posting_emulation_streaming__. With a connection established between Databricks and AWS Kinesis, the stream can be seamlessly processed using `Apache Spark`, enabling concurrent cleaning and writing into `Delta tables` the final destination of the data.

## 3. Tools used

- [Amazon IAM](https://aws.amazon.com/iam/) - Identity and access management to services and resources.

- [Amazon API Gateway](https://aws.amazon.com/api-gateway/) - Amazon API Gateway is a fully managed service that makes it easy for developers to create, publish, maintain, monitor, and secure APIs at any scale. APIs act as the "front door" for applications to access data, business logic, or functionality from your backend services.

- [Amazon Simple Storage Service (Amazon S3)](https://aws.amazon.com/s3/) - It's an object storage service provides unparalleled scalability, data availability, security, and performance. It caters to customers of various sizes and industries, enabling them to securely store and safeguard any volume of data for a wide range of purposes. Whether it's data lakes, cloud-native applications, or mobile apps.

- [Amazon Kinesis](https://aws.amazon.com/kinesis/) - Amazon Kinesis is a fully managed service that efficiently handles the processing and analysis of streaming data at any scale. By utilizing Kinesis, we can  seamlessly ingest real-time data, including video, audio, application logs, website clickstreams, and IoT telemetry data. This data can then be utilized for various applications such as machine learning (ML), analytics, and more.

- [Amazon Elastic Compute Cloud (Amazon EC2)](https://aws.amazon.com/ec2/) - EC2 is a cloud computing platform to run virtual computers and application in the cloud. It provides on-demand, scalable computing capacity to  develop and deploy applications.

- [Amazon Managed Streaming for Apache Kafka (MSK)](https://aws.amazon.com/msk/) - Amazon MSK is a comprehensive managed service that empowers you to develop and operate applications that leverage Apache Kafka for processing streaming data.Amazon MSK, provides access to control-plane operations, including cluster creation, updates, and deletion. Additionally, Apache Kafka data-plane operations can utilized  for efficient data production and consumption.

- [Amazon Managed Workflows for Apache Airflow (MWAA)](https://aws.amazon.com/managed-workflows-for-apache-airflow/) - Workflow orchestration for Apache Airflow.

- [Amazon Relational Database Service](https://aws.amazon.com/rds/) - Relational database in the cloud.

- [Databricks](https://www.databricks.com/) - Databricks delivers a web-based platform for working with Spark, that provides automated cluster management and IPython-style notebooks.

- [Apache Spark](https://spark.apache.org/) - It's a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.

## 4. Installation

Follow the steps to start the process:

First make sure to change the `dummy_settings.ini` file to `settings.ini` and replace the credentials inside.

### 4.1 Set up AWS RDS

 1. Create a `PostgreSQL` database with three tables and upload the the data from the project's `db_data` directory.

    - `pinterest_data.csv`
  	- `geolocation_data.csv`  
  	- `user_data.csv`  

### 4.2 Create MSK Cluster

 1. Access the MSK dashboard and select the "Create Cluster" option from the top right corner to initiate cluster creation.
 2. Make your selection from the available options (`provisioned` reommended), specify the desired `broker type`, and allocate `storage` for each broker. To finalize, click on the "_create cluster_" button.
 3. Choose the cluster that has been created and on the summary page click on the option to "_view client information_".
 4. Copy the following options:
  	- "_Private endpoint_": this is the bootstrap server
  	- "_Plaintext_": zookeper connection string
 5. To enable the EC2 instance to send data to the cluster, simply navigate to the VPC service and under security groups select the default security group associated with the cluster.
 6. Access the "Edit inbound rules" section and proceed to add a new rule. From the Type column, choose "All traffic". In the Source column, input the security group ID of the client machine (locate this in the EC2 console). After saving the rules, the cluster will be configured to accept all traffic originating from the client machine.

### 4.3 Configuring EC2 Kafka client

1. Access the EC2 service and commence the creation of a new EC2 instance by simply clicking on the "_Launch instance_" button.
2. Specify a name for your instance and choose the desired Amazon Machine Image in the Application and OS Images section.
3. Select the instance type, leave the networking and storage options as default.
4. Generate a key-pair and securely store the private key in a `<name_of_file>.pem` file. This particular key will enable secure remote access to the instance.
5. To access the created instance, go to the EC2 dashboard and select the instance. Then, click on the "_connect_" button located at the top right corner. In the new window, you will find instructions on how to connect to the instance using the SSH client.
6. Once connected to the client execute the following commands to install the necessary software and packages:

	- `sudo yum install java-1.8.0`
	- `wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz`
	- `tar -xzf kafka_2.12-2.8.1.tgz`
	- `cd kafka_2.12-2.8.1/bin/`
	- `wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.5/aws-msk-iam-auth-1.1.5-all.jar`
	- `nano ~/.bashrc`
	- copy the following at the end of the file `export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar`, press `ctrl+o` then enter and `ctrl+x`
	- `cd ~ && cd kafka_2.12-2.8.1/bin/`
	- `nano client.properties` and copy the following into the file

```python
# Sets up TLS for encryption and SASL for authN.
security.protocol = SASL_SSL

# Identifies the SASL mechanism to use.
sasl.mechanism = AWS_MSK_IAM

# Binds SASL client implementation.
sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="Your Access Role";

# Encapsulates constructing a SigV4 signature based on extracted credentials.
# The SASL client bound by "sasl.jaas.config" invokes this class.
sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

7. Configure the EC2 client to use AWS IAM for cluster authenticationn: on the AWS IAM console navigate to "_Roles_" and click on "_Create role_", select "_AWS Service_" then select "_MSK Connect_" from the dropdown list. Click on next to define permissions for the Role, choose from "_AWS managed policies_" then click next. Review the role then give a name `<name>-ec2-access-role` and discription and finaly click on "_Create role_".
8. Select the created role and copy it's `ARN`.
9. Go to the `Trust relationships` tab and select `Edit trust policy`, Click on the `Add a principal` button and select `IAM roles` as the Principal type. Replace the `ARN` with the just copied `ARN`.
10. In the `client.properties` file make sure to replace the "_<Your_Access_Role>_" with the above copied `ARN`.
11. Navigate to the Kafka bin folder `cd kafka_2.12-2.8.1/bin/` and create three `Kafka` topics by typing the following on the `EC2 instance`:

	- `./kafka-topics.sh --bootstrap-server <BootstrapServerString> --command-config client.properties --create --topic <topic_name>`  
	Make sure to replace the `<BootstrapServerString>` with the one obtained from the MSK Cluster "_Private endpoint_", and give name to the topic `<name>.pin`,`<name>.geo` and `<name>.user`.

### 4.4 Connect MSK cluster to S3 bucket

1. Go to AWS S3 service page and click on `Create bucket`. Give a desired name and select the region. Leave the recommended default option for ownership and click on `Create bucket`.
2. Create an `IAM role` that can write to the destination bucket. In the `IAM console` under `Access Management` click on `Create role`. Under `Trusted entity type`, select `AWS service`, and under the `Use case` field select S3 in the `Use cases for other AWS services` field. In the permission tab select `Create policy`. On the new tab select `JSON` and replace the policy with the following:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:DeleteObject",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::<DESTINATION_BUCKET>",
                "arn:aws:s3:::<DESTINATION_BUCKET>/*"
            ]
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucketMultipartUploads",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploadParts"
            ],
            "Resource": "*"
        },
        {
            "Sid": "VisualEditor2",
            "Effect": "Allow",
            "Action": "s3:ListAllMyBuckets",
            "Resource": "*"
        }
    ]
}
```

  Skip the rest and click on `Create policy`. Back on the main tab select the just created policy, skip the rest of the pages and click on `Create role`. On the main console select the role just been created and under the `Trust relationships` tab in the `Trusted entities` add the following policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "kafkaconnect.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```

3. Navigate to the `VPC` console and select `Endpoints` under the `Virtual pricate cloud` then click on `Create endpoint`. Under service name choose the desired service and the `Gateway` type. Choose the VPC that corresponds to the MSK cluster's VPC from the drop-down menu in VPC section. Finally, select `Create endpoint`.
4. Creating a custom plugin. On the EC2 instance run the following commands:

	- `cd ~`
	- `sudo -u ec2-user -i`
	- `mkdir kafka-connect-s3 && cd kafka-connect-s3`
	- `wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.0.3/confluentinc-kafka-connect-s3-10.0.3.zip`
	- `aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://<BUCKET_NAME>/kafka-connect-s3/`  
	Make sure to replace the bucket name in the last command with the created bucket's name.

5. Open the MSK console and select `Customised plugins` under the `MSK Connect` section. Click on `Create customised plugin`, copy the bucket's url (obtained from S3 bucket page) where the connector has been uploaded from the EC2 instance and paste it into the `S3 URI`. Give a name to the plugin and click on `Create custom plugin`.
6. In the MSK console, select `Connectors` under the `MSK Connect` section and choose `Create connector`. Select the created plugin then click next. Name the connector and choose the `MSK cluster` from the list. In the connector configuration settings copy the following:

```ini
connector.class=io.confluent.connect.s3.S3SinkConnector
# same region as our bucket and cluster
s3.region=us-east-1
flush.size=1
schema.compatibility=NONE
tasks.max=3
topics.regex=<YOUR_UUID>.*
format.class=io.confluent.connect.s3.format.json.JsonFormat
partitioner.class=io.confluent.connect.storage.partitioner.DefaultPartitioner
value.converter.schemas.enable=false
value.converter=org.apache.kafka.connect.json.JsonConverter
storage.class=io.confluent.connect.s3.storage.S3Storage
key.converter=org.apache.kafka.connect.storage.StringConverter
s3.bucket.name=<BUCKET_NAME>
```

  Make sure to replace the `topics.regex` and the `s3.bucket.name`. After change the connector type to `Provisioned`, set the `MCU count per worker` and `Number of workers` to 1. `Worker Configuration` select `Use a custom configuration` then pick `confluent-worker`. `Access permissions` select the previously created IAM role. Skip the rest then click on the `Create connector`.

### 4.5 Configuring API gateway

1. Navigate to the API Gateway console and click on `Create API`, select `REST API` from the available options and under `Create new API` select `New API`, name it and in the `API endpoint tpe` select `Regional`. Finally click on `Create API`.
2. Select the created API and under `Resouces` click on `Create resource`. Select the `Proxy resource` toggle, for the `Resource name` put `{proxy+}` and finally select `Enable API Gateway CORS` and click on `Create resource`.
3. Select the `ANY` resource and start the integration process by clicking on the `Edit integration`. For the integration type select `HTTP`, also select the `HTTP proxy integration` toggle. For `HTTP method` select `ANY`. For the `Endpoint URL` the Kafka Client Amaxon EC2 instance PublicDNS is required that can be ontained from the EC2 instance page.
The final url will look like `http://KafkaClientEC2InstancePublicDNS:8082/{proxy}`, so make sure the add the `:8082/{proxy}` at the end. Deploy the API and make note of the `invoke url`.
4. On the EC2 instance run the following commands:

	- `cd ~`
	- `sudo wget https://packages.confluent.io/archive/7.2/confluent-7.2.0.tar.gz`
	- `tar -xvzf confluent-7.2.0.tar.gz`
	- `cd confluent-7.2.0/etc/kafka-rest`
	- `nano kafka-rest.properties`
	Modify the `bootstrap.servers` and the `zookeeper.connect` variables with the one obtained in the __4.2 step 4__. Finally add the following to the file:

```ini
# Sets up TLS for encryption and SASL for authN.
client.security.protocol = SASL_SSL

# Identifies the SASL mechanism to use.
client.sasl.mechanism = AWS_MSK_IAM

# Binds SASL client implementation.
client.sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="Your Access Role";

# Encapsulates constructing a SigV4 signature based on extracted credentials.
# The SASL client bound by "sasl.jaas.config" invokes this class.
client.sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

Replace  the "Your Access Role" with the one made at __4.3 step 7__.

5. Start the REST proxy by runnig the following commands:

	- `cd confluent-7.2.0/bin/`
	- `./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties`

6. Run the `user_posting_emulation.py` file in the project's `src` directory.

### 4.6 Batch processing Databricks

1. Set up a Databricks account.
2. Grant the created account full access to the AWS S3 bucket. In the `IAM console` under `Access Management` then `Users` add a new user with the desired name then click next. On the permission page search for `AmazonS3FullAccess` and check the box. Skip everything else until the `Review page` where click on "_Create_user_" botton.
3. Select the created user and under `Security credentials` select `Create access key`. Check the box, give the keypair a description and select `Create access key`. Click on `Download.csv` file.
4. Upload the generated credentials to Databricks by navigating to `Catalog`, click `Add` and `Add data` button. Click on the `Create or modify table` and drop the credentials file. The file will be uploaded to `dbfs:/user/hive/warehouse/`.
5. Go to `Workspace` and on the top right corner click on the dots and select `Import`. Select the three notebooks from the project's `databricks_notebooks` directory.
6. Once imported all three open the `AWS_3_mounting_notebook` and run it.
7. After the S3 bucket has been mounted run the `Batch_data_cleaning_&_querying` notebook.

### 4.7 AWS MWAA

1. Create an S3 bucket for `MWAA`, make sure to `Block all public access` and enable `Bucket versioning`.
2. Go to the `MWAA` console and click on `Create` button, select the appropriate region and choose `Create environment`. Specify the desired environment under `Environment details`. Under `DAG code in Amazon S3` browse  and select the bucket created in the previous step then click next. On the networking page select `Create MWAA VPC`. Choose the preferred `Apache Airflow access mode`, `private network` recommended. Under security groups create a new group. Under `Environment class` select the desired class as well ad the minimum and maximum worker count. FInally click on `Create environment`.
3. Create an API token on Databricks, under `User settings`. Select `Access tokens` and `Generate new token`. Copy the `Token ID`.
4. Open the Airflow UI from the `AWS MWAA` page. Navigate to `Admin` and select `Connections`. Select `databricks_default` and click on `Edit record`. In the `HOST` column copy the account url and in the `Extra` column add `{"token": "<token_from_previous_step>", "host": "<url_from_host_column>"}`. In the `Connection Type` columns select __Databricks__ from the drop-down menu. 5. If the connection type missing from the previous step install the following:

	- `git clone https://github.com/aws/aws-mwaa-local-runner.git`
	- `cd aws-mwaa-local-runner`
	- `./mwaa-local-env build-image`
	- `./mwaa-local-env start`
	Open the `Airflow UI` at `http://localhost:8080/` with username __admin__ and password __test__. Navigate to `aws-mwaa-local-runner/requirements/` where create a `requirements.txt` file and add the following line `apache-airflow[databricks]`. Run the command
	- `./mwaa-local-env test-requirements`
	Upload the file to the S3 bucket, created in __step 1__. Navigate to MWAA console and select the __Environment__. Select `Edit` and under the `DAG code in Amazon S3` update the `Requirements file` by selecting the path to the `requirements.txt` file.
	- On the `Airflow UI` select `Admin` and `Connections`, click on `Edit connection` on the `default_databricks` and make sure the _connection_id, connection_type, host and extra_ fields are not empty.

6. Upload the `_dag.py` file from the project's `src` directory to the S3 bucket under the `dags` folder. Before that modify the `notebook_path` and `cluster_id` in the file. The notebook_path is wthe path of the `Batch_data_cleaning_&_querying` on Databricks and the cluster_id is a running cluster on Databricks.
7. On the `Airflow UI` unpause the dag and trigger it manually.

### 4.8 AWS Kinesis

1. Go to the `Kinesis` console on AWS and click on the `Create data stream`. Give a name to the stream, select `Capacity mode` and click on `Create data stream`. Make sure to create three streams.
2. Navigate to `IAM` console and create a new role. Choose the `AmazonKinesisFullAccessRole` policy.
3. Modify the API create in step __4.5__. Create a new resource with name `streams` and then create a new `GET` method. In the create method page type the following:

	- `Integration Type` : `AWS service`
	- `AWS Region` : the region where all the services are for this project.
	- `AWS Service` : `Kinesis`
	- `HTTP method` : `POST`
	- `Action Type` : `User action name`
	- `Action name` : `Liststreams`
	- `Execution role` : copy the `ARN` of the Kinesis access rolecreated in step 1
	Click on `Create method`. On the `Method Execution` page select the `Integration panel` and click on `Edit`. Click on the `URL request headers parameters` and `Add request header parameter` button:
	- `Name` : `Content-Type`
	- `Mapped form` : `application/x-amz-json-1.1`
	Expand the `Mapping Templates` panel and click on `Add mapping template`:
	- `Content-Type` : `application/json`
	- `Template body` : `{}`  
4. Add a new resource under `streams` with name `{stream-name}` and add three new methods `POST`, `GET` and `DELETE`. Follow the same steps as above in __step 3__ but modify the following:  

	__GET__
	- `Action name` : `DescribeStream`
	- `Template body` :

	```json
	{
	"StreamName": "$input.params('stream-name')"
	}
	```

	__POST__
	- `Action name` : `CreateStream`
	- `Template body` :

	```json
	{
	"ShardCount": #if($input.path('$.ShardCount') == '') 5 #else $input.path('$.ShardCount') #end,
	"StreamName": "$input.params('stream-name')"
	}
	```

	__DELETE__
	- `Action name` : `DeleteStream`
	- `Template body` :

	```json
	{
    "StreamName": "$input.params('stream-name')"
	}
	```

5. Adding two new child resources under the `{stream-name}` with names `record` and `records` and create one method under each:  

	__PUT__ for `record`
	- `Action name` : `PutRecord`
	- `Template body` :

	```json
	{
    "StreamName": "$input.params('stream-name')",
    "Data": "$util.base64Encode($input.json('$.Data'))",
    "PartitionKey": "$input.path('$.PartitionKey')"
	}
	```

	__PUT__ for `records`
	- `Action name` : `PutRecord`
	- `Template body` :

	```json
	{
    "StreamName": "$input.params('stream-name')",
    "Records": [
       #foreach($elem in $input.path('$.records'))
          {
            "Data": "$util.base64Encode($elem.data)",
            "PartitionKey": "$elem.partition-key"
          }#if($foreach.hasNext),#end
        #end
    	]
	}
	```
6. Run the `user_posting_emulation_streaming.py` file in the project's `src` directory.
7. Run the `AWS_Kinesis_data_streaming` notebook on Databricks to process the data and save it.

__IMPORTANT__  
Make sure in all steps the region is the same for all services, all the credentials are replace, name of streams and buckets have been changed during the installation.

## 5. File structure

The project's structure:

```text
📦pinterest-data-pipeline754
 ┣ 📂creds
 ┃ ┣ 📜dummy_settings.ini
 ┣ 📂databricks_notebooks
 ┃ ┣ 📜AWS_Kinesis_data_streaming.ipynb
 ┃ ┣ 📜AWS_S3_mounting_Notebook.ipynb
 ┃ ┗ 📜Batch_data_cleaning_&_querying.ipynb
 ┣ 📂db_data
 ┃ ┣ 📜geolocation_data.csv
 ┃ ┣ 📜pinterest_data.csv
 ┃ ┗ 📜user_data.csv
 ┣ 📂images
 ┃ ┣ 📜batch_q1.png
 ┃ ┣ 📜batch_q2.png
 ┃ ┣ 📜batch_q3a.png
 ┃ ┣ 📜batch_q3b.png
 ┃ ┣ 📜batch_q4.png
 ┃ ┣ 📜batch_q5.png
 ┃ ┣ 📜batch_q6.png
 ┃ ┣ 📜batch_q7.png
 ┃ ┣ 📜batch_q8.png
 ┃ ┗ 📜cloud-pinterest-pipeline.webp
 ┣ 📂src
 ┃ ┣ 📜12a740a19697_dag.py
 ┃ ┣ 📜user_posting_emulation.py
 ┃ ┗ 📜user_posting_emulation_streaming.py
 ┣ 📜.gitignore
 ┣ 📜LICENSE
 ┗ 📜README.md
```

## 6. Licence

MIT License - See LICENCE
