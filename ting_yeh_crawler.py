from bs4 import BeautifulSoup
import re
import requests

request_html = requests.get("https://www.dss.virginia.gov/facility/search/alf.cgi?rm=Details;ID=22915;")
soup = BeautifulSoup(request_html.text, 'html.parser')

#inspection_info1
inspection_info1= soup.find_all("u")
inspection_info1_list=[]

for tag in inspection_info1:
  inspection_info1_list.append(tag.string)

#inspection_info2
inspection_info2 = soup.find_all("td",valign="top")
inspection_info2_list=[]

for tag in inspection_info2:
  inspection_info2_list.append(tag.string)

#facility(name & address)
facility_info1= soup.find("td",valign="top")
facility_info1_list=[]

for tag in facility_info1:
  facility_info1_list.append(tag.string)

#facility(city,state,zipcode & phone number)
facility_info2= soup.find_all("td",valign="top")
facility_info2_list=[]

for tag in facility_info2:
  facility_info2_list.append(tag.string)

#facility_info
facility_name = facility_info1_list[1].strip()
facility_address = facility_info1_list[3].strip()
facility_location = facility_info2_list[1].strip()
facility_phone_number = facility_info2_list[1].strip()

#inspection_info
facility_type =	inspection_info1_list[0]
license_type = inspection_info1_list[1]
expiration_date =	inspection_info2_list[8].strip()
qualification = inspection_info1_list[3]+" , "+inspection_info1_list[4]
administrator =	inspection_info2_list[11].strip()
capacity =	inspection_info2_list[13].strip()
inspector = inspection_info2_list[15].strip()

#facility_info
facility_dict = {
'facility_name':facility_name,
'facility_address':facility_address,
'facility_location':facility_location,
'facility_phone_number':facility_phone_number
}

#inspection_dict
inspection_dict = {
'facility_type':facility_type,
'license_type':license_type,
'expiration_date':expiration_date,
'qualification':qualification,
'administrator':administrator,
'capacity':capacity,
'inspector':inspector,
}

print(facility_dict)
print(inspection_dict)