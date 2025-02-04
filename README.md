# BIG DATA ANALYTICS AND VISUALIZATION: TRACKING STUDENT PROGRESS

## Overview
This system integrates Docker, Kafka, Elasticsearch, Kibana, and a Web Application to track and visualize student progress effectively. 

## System Architecture
### Data Source
- Input data comes from Excel files containing student information, course names, course codes, and grades.
- The data is well-structured and updated regularly, serving as the primary data source for the system.

### Docker
- Docker is used to package and deploy Kafka, simplifying instance management and scaling.
- Configuration is managed using a `docker-compose.yml` file for streamlined deployment.

### Kafka
- **Producer:** Reads and processes Excel files, sending data to Kafka Topics.
- **Topic:** Categorizes messages, acting as a pipeline for data processing.
- **Broker:** Kafka brokers store and manage messages, ensuring reliability and scalability.
- **Consumer:** Retrieves messages from topics and sends them to Elasticsearch for storage and analysis.

### Elasticsearch
- Data from Kafka is ingested into Elasticsearch via Kafka Connect, allowing preprocessing before indexing.
- Elasticsearch enables fast querying, searching, and analysis of student progress data.

### Kibana
- Provides visualization of student progress through dashboards and reports.
- Displays trends, score distributions, and other essential statistics.

### Web Application (Streamlit)
- Built using **Streamlit**, a Python-based framework for interactive UI development.
- Connects to Elasticsearch via RESTful API for real-time student progress queries.
- Enables users to search student information, view scores, and track academic performance.

## Deployment
1. **Setup Docker**: Ensure Docker is installed and running.
2. **Deploy Kafka**: Use Docker Compose to launch Kafka instances.
3. **Start Producers & Consumers**: Send and retrieve student data between Kafka and Elasticsearch.
4. **Launch Elasticsearch & Kibana**: Store and visualize student progress data.
5. **Run Streamlit App**: Provide an interactive interface for data retrieval and insights.

## Key Features
- **Real-time Data Processing:** Kafka ensures efficient data flow from Excel to Elasticsearch.
- **Fast Search & Analysis:** Elasticsearch enables quick queries and deep analytics.
- **Intuitive Visualizations:** Kibana offers interactive dashboards and reports.
- **User-Friendly Interface:** Streamlit provides an easy-to-use UI for students and administrators.

## Contributors
- **Huynh Chau** (Email: chau25202@gmail.com)

## License
This project is licensed under the MIT License.
