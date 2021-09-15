#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 8 10:05:02 2021

"""
# =============================================================================
# # import
# =============================================================================
import sqlite3
from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
from urllib.request import urlopen


# =============================================================================
#   DB Browser for SQLite
# =============================================================================
conn = sqlite3.connect('/Users/timyang/NLP_project/NLP_database.db')
c = conn.cursor()

# =============================================================================
#   Information of inspection and facilities (table1)
# =============================================================================

#facility_info
facility_id1 = None
facility_name = None
facility_address = None
facility_location = None
facility_phone_number = None

#inspection_info
facility_type =	None
license_type = None
expiration_date = None
qualification = None
administrator =	None
business_hours = None
capacity =	None
inspector = None

search_url=[]
facility_url=[]

for i in range(1,552,25):
    search_url.append("https://www.dss.virginia.gov/facility/search/alf.cgi?rm=Search;;Start="+str(i))

for url in search_url:
    html = urlopen(url)
    bsObj = BeautifulSoup(html, 'html.parser')
    t1 = bsObj.find_all('a')
    for t2 in t1:
        t3 = t2.get('href')
        t3 = str(t3)
        if "Details" in t3:
            facility_url.append("https://www.dss.virginia.gov"+t3)

for url in facility_url:
    df = pd.read_html(url)

    #facility_info
    request_html = requests.get(url)
    soup = BeautifulSoup(request_html.text, 'html.parser')

    #facility(name & address)
    facility_info= soup.find("td",valign="top")
    facility_info_list=[]

    for tag in facility_info:
        facility_info_list.append(tag.string)

    #inspection(administrator, capacity,inspector)
    inspection_info = soup.find_all("td",valign="top")
    inspection_info_list=[]

    for tag in inspection_info:
        inspection_info_list.append(tag.string)

    facility_id1 = int(re.findall(r"ID=(.*);",url)[0])
    facility_name = facility_info_list[1].strip()
    facility_address = facility_info_list[3].strip()
    facility_location = df[0][0][1]
    facility_phone_number = df[0][0][2]

    #inspection_info
    facility_type =	df[1][1][0]
    license_type = df[1][1][1]
    expiration_date = df[1][1][2]

    for i in range(3,10):
        try:
            if 'Qualification' in df[1][0][i]:
                qualification =	df[1][1][i]
            elif 'Administrator' in df[1][0][i]:
                administrator =	df[1][1][i]
            elif 'Hours' in df[1][0][i]:
                business_hours = df[1][1][i]
            elif 'Capacity' in df[1][0][i]:
                capacity =	df[1][1][i]
            elif 'Inspector' in df[1][0][i]:
                inspector = df[1][1][i]
        except:
            err = None

    table1=[facility_id1,facility_name,facility_address, facility_location ,facility_phone_number, facility_type, license_type, expiration_date, qualification, administrator, business_hours, capacity, inspector]
    c.execute('insert into Facility_info(facility_id, facility_name, facility_address, facility_location ,facility_phone_number, facility_type, license_type, expiration_date, qualification, administrator, business_hours, capacity, inspector) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', table1)
    conn.commit()
# =============================================================================
#   Information of violations (table2)
# =============================================================================
    html = urlopen(url)
    bsObj = BeautifulSoup(html, 'html.parser')
    t1 = bsObj.find_all('a')
    violations_url=[]
    for t2 in t1:
        t3 = t2.get('href')
        t3 = str(t3)
        if ";#Violations" in t3:
            violations_url.append("https://www.dss.virginia.gov"+t3)

    for vio_url in violations_url:
        request_html = requests.get(vio_url)
        soup = BeautifulSoup(request_html.text, 'html.parser')
        info= soup.find("div",id="main_content").find("p").text
        df = pd.read_html(vio_url)

        standard=[]
        description=[]
        plan_of_correction=[]

        if len(df) >= 3 and len(df[2]) >= 2:
            for i in range(0,len(df[2][1])):
                try:
                    if 'Standard' in df[2][0][i]:
                        standard.append(df[2][1][i])
                    elif 'Description' in df[2][0][i]:
                        description.append(df[2][1][i])
                    elif 'Correction' in df[2][0][i]:
                        plan_of_correction.append(df[2][1][i])
                except:
                    err = None
        else:
            continue

        inspection_id = int(re.findall(r"Inspection=(.*);",vio_url)[0].split(";")[0])
        facility_id2 = int(re.findall(r"ID=(.*);",vio_url)[0].split(";")[0])
        facility_name = soup.find("b").text
        current_inspector =re.findall(r"Inspector:(.*)Inspection",info.replace("\n", ""))[0].strip()
        inspection_date =re.findall(r"Date:(.*)Complaint",info.replace("\n", ""))[0].strip().replace("\t","").replace("and"," and ")
        complaint_related =re.findall(r"Related:(.*)Areas",info.replace("\n", ""))[0].strip()
        areas_reviewed = df[0][0][0]
        comments = df[1][1][0]

        violation_list=[]
        violation_list.append(inspection_id)
        violation_list.append(facility_id2)
        violation_list.append(facility_name)
        violation_list.append(current_inspector)
        violation_list.append(inspection_date)
        violation_list.append(complaint_related)
        violation_list.append(areas_reviewed)
        violation_list.append(comments)

        for i in range(0,len(standard)):
            violation_list.append(standard[i])
            violation_list.append(description[i])
            violation_list.append(plan_of_correction[i])
            c.execute('insert into Violation_info(inspection_id, facility_id, facility_name, current_inspector, inspection_date ,complaint_related, areas_reviewed, comments, standard, description, plan_of_correction) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', violation_list)
            conn.commit()
            violation_list.pop(6)
            violation_list.pop(6)
            violation_list.pop(6)
conn.close()
