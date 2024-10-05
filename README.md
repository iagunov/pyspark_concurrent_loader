# PySpark concurrent loader.

This project is a Spark-based data processing application designed to efficiently handle large datasets by loading data from multiple sources in parallel. It uses ThreadPoolExecutor to process data concurrently and writes results directly into a unified target table, eliminating the need for intermediate storage. The application supports dynamic partitioning to enable efficient data overwriting, comprehensive logging for tracking processes, and error handling at a granular level. Additionally, it features automatic journal writing for status updates and a customizable Spark session configuration to optimize performance for different workloads.

### Features
- Loading data from multiple sources in parallel using ThreadPoolExecutor
- Parallel writing directly to a unified result table without intermediate storage.
- Dynamic partition overwrite with Spark to optimize data handling.
- Granular error handling for individual data sources.
- Comprehensive logging to track process status and errors.
- Automatic journal writing for status updates and process results.
- Configurable Spark session with customizable parameters.

### Project Status
**This project is currently under development.** Features and functionality are subject to change as development progresses.

### Requirements
- Python 3.6+
- Apache Spark 3.0+
- PySpark

### Contact
Feel free to reach out with any questions or feedback.

- Linkedin: https://www.linkedin.com/in/vasilii-iagunov/
- Email: iagunov.vasilii@gmail.com