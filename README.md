This repo is part of my Dissertation where the focus was to report the intricacies of designing, building, and deploying micro-scale Data Pipelines in AWS. However, the provider has a wide range of solutions to execute similar tasks. In addition, SMEs were the target audience since Cloud Computing is meant to be affordable, and enterprises are looking to be as frugal as possible. The report produced two different architectures at the processing layer with similar secondary services that shape the standard Data Pipeline. The processing layer entails data extraction, transformation, and loading. However, this process is usually conducted in steps, known as data workflows. The workflows for Data Pipelines are often organised as directed acyclic graphs (DAGs). No loops are allowed among the tasks. These design criteria were pivotal in selecting the services to be analysed. Moreover, the services needed to have low maintenance and provision additional resources automatically. Therefore, AWS Managed Workflows for Apache Airflow (MWAA) & AWS Step-Functions in conjunction with AWS Lambda were selected based on the design standards. Notice that MWAA is a hybrid version of Airflow and the other one is Cloud proprietary technology. The results were interesting as claims from AWS were proven to be right as their proprietary technology is faster and inexpensive at micro-scale. SMEs should only opt for MWAA if the investment return is positive since users have to pay for idle time to keep resources running. The bottom line is that both technologies are great, yet the use case will always dictate the final decision.
