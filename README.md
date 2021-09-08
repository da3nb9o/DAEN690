# NLP of Va. Dept of Social Services Long-term Care Facility (LCF) Audit Reports

## Introduction

### Background
Virginia Social Services (VDSS) is a system of regional supervision and management services. Providing supervision and guidelines for 120 regional offices throughout the country, VDSS offers a variety of services and benefits for Virginia, more than 17 million people each year. The VDSS program is designed to help Virginia's most vulnerable citizens find a permanent solution to many challenges of life. This department is responsible for managing various programs, including temporary support for poor families (TANF), support programs for complementary nutrition (SNAP), Medicaid, adoption, parental support, the Regeneration of refugees and services to protect children and adults.

Virginia, with approximately 19,000 hospital beds has 566 licensed Long-term Care Facilities (LCFs). LCFs must operate according to Commonwealth of Virginia, Chapter 73, Title 22 standards for licensed assisted living facilities. During 2017-2019, Virginia generated more than 20,000 breach audit reports (11% related to drug use and compliance). A study of 100 narrative audit reports randomly selected by Motti & Sherry (2021) identified eight categories of drug regulatory violations.

To solve this problem and better understand it, we are aiming to generate a statistics on Medicine Administration Violations form LFC’s audit reports

### Problem Space
A bulge in the aging population and improved healthcare has resulted in increased demand for services at LCFs. Virginia generated over 20,000 violation-related audit reports in the period 2017-2020, and 11% of them are related to medication administration and adherence.

Although they collected vast documentations, there is no method, tool, or visual analytics approach to facilitate the interpretation of violations and their causes, through data mining in error reports. Still, the analysis of violations is crucial to understand and characterize violations, and propose measures and systems aimed to prevent future incidents. 

In the previous study, they analyzed a random sample of 100 violations extracted from the Department of Social Services (DSS) reports from Virginia. They categorized the violations per root cause: administration, documentation, access, availability, and training. The characterizing of common medication errors lays the scientific foundations to develop an NLP model to automate the analysis of reports, synthesize report contents, and facilitate the interpretation of violations. The results show that most violations refer to wrong administration, and that issues with access, availability and storage could be easily preventable.
The VA DSS would like to analyze all 20000 narrative Audit Reports related to Medicine Administration and generate statistics to provide more insights and understanding of the errors, and it is the goal of our research.

With the result of the research, it would be helpful to ensure that the medication regimen is followed in compliance with state laws to prevent errors. Also, it is important to consider that residents of long-term living facilities are not always capable of caring for themselves, therefore residents with severe cognitive impairments require trusted support staff members to care for them. Otherwise, there are fatal consequences associated with wrong medication intake, leading to health issues, additional costs, or even death risks.

### Research
In the meeting with sponsor, they provide the thoughts regarding data collection and data preparation for NLP modeling. What we are going to do next is to parse the documents from data sources and try to extract and identify entities in order to be prepared for further NLP modeling. Sponsor also provides learning materials relating to the knowledge of NLP modeling. Through the materials, we will learn how to label sentences for NLP classification problem, how to extract entities, and how to train classification model to determine entity relationship.


### Project Objectives
This project goal is generated statistics on Medicine Administration Violations from narrative Long-term Care Facility (LCF) Audit reports.

In this process, our team will learn how to collect data of real-world including structure and unstructured data. Then, we will learn how to deal with them to transform to useful statistics information by text mining. Finally, we will learn how to present the result of NLP by several visualization techniques.

After our team finish this project, we will learn to cooperate with teammates in agile team. We will learn to provide a better output through two weeks sprints and try to identify and correct problems and defects quickly. In addition, we will learn to utilize YouTrack and GitHub to manage our version and task clearly. In terms of understanding about the problem, we can have a consensus through regular discussions and flexible team atmosphere.

Finally, we believe that our product of this project will provide valuable information to LCFs. They can target medication administration and adherence to develop corresponding strategies with limited resources to reduce the violation. It will helpful for the LCFs in Virginia to keep their license. We hope that the targeted group will expanse to the entire United States of LCFs in the future.

## Data Acquisition
### Overview:

Dataset Name: DSS Audit Report
Dataset Owner: VA. DSS Website
Dataset Type: open source
Dataset Size: 500 M 

### Field Descriptions:

- Facility type (Type: string) - The type of facility listed on the DSS website, including Non-Ambulatory, Assisted Living Care and Special Care Unit. This field is not allowed to be null.
-	Date range start (Type: datetime) - The start date of the inspection date of the facility (format: YYYY-MM-DD). The time zone is assumed to be Eastern Standard Time. This field is not allowed to be null.
-	Date range end (Type: datetime) - The end date of the inspection date of the facility (format: YYYY-MM-DD). The time zone is assumed to be Eastern Standard Time. This field is not allowed to be null.
-	Standard (Type: string) – The violation code listed on the DSS website. The type of violation code is string. This field is not allowed to be null.
-	Count of violations (Type: integer) – The counts of violations listed on the DSS website. This column is about number of violations. The counts of violations is integer. This field is not allowed to be null.
-	Count of Inspections (Type: integer) – The count of Inspections Number of inspections listed on the DSS website. The type of Count of Inspections is integer. This field is not allowed to be null.


## Appendix B: Risk Section

|RISK NAME|DESCRIPTION|PROBABILITY|IMPACT|MITIGATION|
|---|---|---|---|---|
|Server crashed|Too many user cause the server crashed|Low|High|Evaluate the sever when building the dataset|
|Change of website|The visualization website will crash if the department website is changed|Medium|High|Need people to maintain web crawler|
|Cyber attack|The web server may be attacked by hackers, like DDoS|High|Low|Using addition service, like Cloudflare|
|Finance|Need money to maintain the server|High|High|Find the free service, like AWS education|
|Update cycle|The website update the information too slow|Medium|Low|To find how often the dataset be update. Try to find the balance between budget and efficient|
