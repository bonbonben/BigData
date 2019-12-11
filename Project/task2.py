#!/usr/bin/env python
# coding: utf-8

import os, sys
import pyspark
import string
import json
import re

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.functions import upper, lower, col

def predict_result(col_list, pre_name, index):
    if pre_name != "other":
        for i in range(len(col_list)):
            temp = col_list[i][0]
            if predict_by_name(pre_name, temp) == pre_name:
                value_arr[index] += 1
            else:
                s = predict_by_name(pre_name, temp)
                for t in range(22):
                    if s == value_name[t]:
                        value_arr[t] += 1
    else:
        for i in range(len(col_list)):
            temp = col_list[i][0]
            temp = temp.strip()
            temp = temp.split(' ')
            s = check_other_category(temp)
            for t in range(22):
                if s == value_name[t]:
                    value_arr[t] += 1

def predict_by_name(pre_name, value):
    phone_pattern = r"^\s*(\+?(\d{1,3}))?[-. (]*\(?\d{3}\)?[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$"
    lat_lon_cord_pattern = r"^\s*\(?-?(\d{0,3}\.\d),\s*-?(\d{0,3}\.\d)\)\s*$"
    zip_code_pattern = r"^\s*\d{5}-?(\d{4})?\s*$"
    building_classification_pattern = r"^\s*[A-Za-z]\d{1}-.*\s*$"
    #website_pattern = r"^\s*((WWW\.|HTTP(s)?://)?.*(\.NET|\.ORG|\.COM)?/?)+\s*$"
    website_pattern = re.compile(r'^(https?:\/\/)?(www\.)?([a-zA-Z0-9]+(-?[a-zA-Z0-9])*\.)+[\w]{2,}(\/\S*)?$', re.IGNORECASE)
    special_char_pattern = re.compile(r'^[|\^&+%*#=!>0-9]+.*$')

    s = value.strip()
    s = s.split(' ')    
    if pre_name == "person_name":
        if re.match(special_char_pattern, s):
            return "other"
        else:
            return "person_name"
    elif pre_name == "business_name":
        return "business_name"
    elif pre_name == "phone_number":
        if re.match(phone_pattern, value):
            return "phone_number"
        else:
            return check_other_category(s)
    elif pre_name == "lat_lon_cord":
        if re.match(lat_lon_cord_pattern, value):
            return "lat_lon_cord"
        else:
            return check_other_category(s)
    elif pre_name == "zip_code":
        if re.match(zip_code_pattern, value):
            return "zip_code"
        else:
            return check_other_category(s)
    elif pre_name == "website":
        if re.match(website_pattern, value):
            return "website"
        else:
            return check_other_category(s)
    elif pre_name == "address":
        for i in range(len(s)):
            if s[i].upper() in address_list:
                return "address"
            else:
                return check_other_category(s)
    elif pre_name == "street_name":
        for i in range(len(s)):
            if s[i].upper() in address_list:
                return "street_name"
            else:
                return check_other_category(s)
    elif pre_name == "borough":
        for i in range(len(s)):
            if s[i].upper() in borough_list:
                return "borough"
            else:
                return check_other_category(s)
    elif pre_name == "city":
        for i in range(len(s)):
            if s[i].upper() in city_list:
                return "city"
            else:
                return check_other_category(s)
    elif pre_name == "neighborhood":
        for i in range(len(s)):
            if s[i].upper() in neighborhood_list:
                return "neighborhood"
            else:
                return check_other_category(s)
    elif pre_name == "color":
        for i in range(len(s)):
            if s[i].upper() in color_list:
                return "color"
            else:
                return check_other_category(s)
    elif pre_name == "city_agency":
        for i in range(len(s)):
            if s[i].upper() in city_agency_list:
                return "city_agency"
            else:
                return check_other_category(s)
    elif pre_name == "area_of_study":
        for i in range(len(s)):
            if s[i].upper() in area_of_study_list:
                return "area_of_study"
            else:
                return check_other_category(s)
    elif pre_name == "school_name":
        for i in range(len(s)):
            if s[i].upper() in school_name_list:
                return "school_name"
            else:
                return check_other_category(s)
    elif pre_name == "subject_in_school":
        for i in range(len(s)):
            if s[i].upper() in subject_in_school_list:
                return "subject_in_school"
            else:
                return check_other_category(s)
    elif pre_name == "school_level":
        for i in range(len(s)):
            if s[i].upper() in school_level_list:
                return "school_level"
            else:
                return check_other_category(s)
    elif pre_name == "college_name":
        for i in range(len(s)):
            if s[i].upper() in college_name_list:
                return "college_name"
            else:
                return check_other_category(s)
    elif pre_name == "car_make":
        for i in range(len(s)):
            if s[i].upper() in car_make_list:
                return "car_make"
            else:
                return check_other_category(s)
    elif pre_name == "vehicle_type":
        for i in range(len(s)):
            if s[i].upper() in vehicle_type_list:
                return "vehicle_type"
            else:
                return check_other_category(s)
    elif pre_name == "location_type":
        for i in range(len(s)):
            if s[i].upper() in location_type_list:
                return "location_type"
            else:
                return check_other_category(s)     
    elif pre_name == "park_playground":
        for i in range(len(s)):
            if s[i].upper() in park_playground_list:
                return "park_playground"
            else:
                return check_other_category(s)
    else:
        return check_other_category(s)

def check_other_category(s):
    phone_pattern = r"^\s*(\+?(\d{1,3}))?[-. (]*\(?\d{3}\)?[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$"
    lat_lon_cord_pattern = r"^\s*\(?-?(\d{0,3}\.\d),\s*-?(\d{0,3}\.\d)\)\s*$"
    zip_code_pattern = r"^\s*\d{5}-?(\d{4})?\s*$"
    building_classification_pattern = r"^\s*[A-Za-z]\d{1}-.*\s*$"
    #website_pattern = r"^\s*((WWW\.|HTTP(s)?://)?.*(\.NET|\.ORG|\.COM)?/?)+\s*$"
    website_pattern = re.compile(r'^(https?:\/\/)?(www\.)?([a-zA-Z0-9]+(-?[a-zA-Z0-9])*\.)+[\w]{2,}(\/\S*)?$', re.IGNORECASE)

    for i in range(len(s)):
        if s[i].upper() in address_list:
            return "address"
        elif s[i].upper() in address_list:
            return "street_name"
        elif s[i].upper() in borough_list:
            return "borough"
        elif s[i].upper() in city_list:
            return "city"
        elif s[i].upper() in neighborhood_list:
            return "neighborhood"
        elif s[i].upper() in color_list:
            return "color"
        elif s[i].upper() in city_agency_list:
            return "city_agency"
        elif s[i].upper() in subject_in_school_list:
            return "subject_in_school"
        elif s[i].upper() in area_of_study_list:
            return "area_of_study"
        elif s[i].upper() in school_level_list:
            return "school_level"
        elif s[i].upper() in school_name_list:
            return "school_name"
        elif s[i].upper() in college_name_list:
            return "college_name"
        elif s[i].upper() in car_make_list:
            return "car_make"
        elif s[i].upper() in vehicle_type_list:
            return "vehicle_type"
        elif s[i].upper() in location_type_list:
            return "location_type"
        elif s[i].upper() in park_playground_list:
            return "park_playground"
        else:
            value = s[0]
            if re.match(phone_pattern, value):
                return "phone_number"
            elif re.match(lat_lon_cord_pattern, value):
                return "lat_lon_cord"
            elif re.match(zip_code_pattern, value):
                return "zip_code"
            elif re.match(website_pattern, value):
                return "website"
            else: 
                return "other"

def put_to_dict(v1, n1):
    list_ = []
    t1 = dict()
    if n1 != "other":
        t1['semantic_type'] = n1
        t1['count'] = v1
        list_.append(t1)
    else:
        t1['semantic_type'] = n1
        t1['label'] = "undefined"
        t1['count'] = v1
        list_.append(t1)
    return list_

if __name__ == "__main__":

    sc = SparkContext()

    spark = SparkSession \
        .builder \
        .appName("proj") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)

    outputJSON = []

    with open("cluster3.txt", 'r') as f:
        cols = f.read().replace('\n', '').replace(' ', '').replace("'", '')
    cols = cols[1:-1]
    file_list = cols.split(',')

    file_id = 0
    path = '/user/hm74/NYCColumns/'
    
    for file in file_list:
        temp_output = dict()
        temp_output['column_name'] = file
        semantic_types = []
        temp_semantic_types = dict()
        if file == "w7w3-xahh.Address_City.txt.gz":
            inFile = path + file
            print("File Name: ", file)
            fileName = file.split('.')[1]
            fileName = fileName.lower()

            spark = SparkSession(sc)
            df = sqlContext.read.format('csv').options(inferschema='true', delimiter='\t').load(inFile)
            col = df.collect()

            person_name_v = 0 
            business_name_v = 0
            phone_number_v = 0
            address_v = 0
            street_name_v = 0
            city_v = 0
            neighborhood_v = 0
            lat_lon_cord_v = 0
            zip_code_v = 0
            borough_v = 0
            school_name_v = 0
            color_v = 0
            car_make_v = 0
            city_agency_v = 0
            area_of_study_v = 0
            subject_in_school_v = 0
            school_level_v = 0
            college_name_v = 0
            website_v = 0
            building_classification_v = 0
            vehicle_type_v = 0
            location_type_v = 0
            park_playground_v = 0
            value_arr = [person_name_v, business_name_v, phone_number_v, address_v, street_name_v, city_v, neighborhood_v, lat_lon_cord_v, zip_code_v, borough_v, school_name_v, \
                        color_v, car_make_v, city_agency_v, area_of_study_v, subject_in_school_v, school_level_v, college_name_v, website_v, building_classification_v, \
                        vehicle_type_v, location_type_v, park_playground_v]
            value_name = ["person_name", "business_name", "phone_number", "address", "street_name", "city", "neighborhood", "lat_lon_cord", "zip_code", "borough", "school_name", \
                        "color", "car_make", "city_agency", "area_of_study", "subject_in_school", "school_level", "college_name", "website", "building_classification", \
                        "vehicle_type", "location_type", "park_playground"]
            #person_name
            if re.match(r"^.*(first.*name|last.*name).*", fileName):
                predict_result(col, "person_name", 0)
            #business_name
            elif re.match(r"^.*(business.*name|org.*name|dba).*", fileName):
                predict_result(col, "business_name", 1)
            #phone_number
            elif re.match(r"^.*phone|fax.*", fileName):
                predict_result(col, "phone_number", 2)
            #address
            elif re.match(r"^.*address.*", fileName):
                predict_result(col, "address", 3)
            #street_name
            elif re.match(r"^.*street.*", fileName):
                predict_result(col, "street_name", 4)
            #city
            elif re.match(r"^.*city.*", fileName):
                predict_result(col, "city", 5)
            #neighborhood
            elif re.match(r"^.*neighborhood.*", fileName):
                predict_result(col, "neighborhood", 6)
            #lat_lon_cord
            elif re.match(r"^.*location.*", fileName):
                predict_result(col, "lat_lon_cord", 7)
            #zip_code
            elif re.match(r"^.*zip.*", fileName):
                predict_result(col, "zip_code", 8)
            #borough
            elif re.match(r"^.*boro.*", fileName):
                predict_result(col, "borough", 9)
            #school_name
            elif re.match(r"^.*school.*name.*", fileName):
                predict_result(col, "school_name", 10)
            #color
            elif re.match(r"^.*color.*", fileName):
                predict_result(col, "color", 11)
            #car_make
            elif re.match(r"^.*vehicle.*make.*", fileName):
                predict_result(col, "car_make", 12)
            #city_agency
            elif re.match(r"^.*agency.*", fileName):
                predict_result(col, "city_agency", 13)
            #area_of_study
            elif re.match(r"^.*interest.*", fileName):
                predict_result(col, "area_of_study", 14)
            #subject_in_school
            elif re.match(r"^.*subject.*", fileName):
                predict_result(col, "subject_in_school", 15)
            #school_level
            elif re.match(r"^.*school.*level.*", fileName):
                predict_result(col, "school_level", 16)
            #college_name
            elif re.match(r"^.*college.*", fileName):
                predict_result(col, "college_name", 17)
            #website
            elif re.match(r"^.*website.*", fileName):
                predict_result(col, "website", 18)
            #building_classification
            elif re.match(r"^.*building.*classification.*", fileName):
                predict_result(col, "building_classification", 19)
            #vehicle_type
            elif re.match(r"^.*vehicle.*type.*", fileName):
                predict_result(col, "vehicle_type", 20)
            #location_type
            elif re.match(r"^.*typ.*desc.*", fileName):
                predict_result(col, "location_type", 21)
            #park_playground
            elif re.match(r"^.*(park|school).*", fileName):
                predict_result(col, "park_playground", 22)
            #other
            else:
                predict_result(col, "other", 23)

            counter = 0
            total = len(col)
            for i in range(22):
                if value_arr[i] != 0:
                    semantic_types += put_to_dict(value_arr[i], value_name[i])
                    total -= value_arr[i]
                if total == 0:
                    break
            if total != 0:
                semantic_types += put_to_dict(total, "other")

            temp_output['semantic_types'] = semantic_types
            print(temp_output)

        borough_list = ["K", "M", "Q", "R", "X", "BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN", "ISLAND", "STATEN ISLAND"]
        school_level_list = ['K-2', 'K-3', 'K-8', 'ELEMENTARY', 'MIDDLE', 'HIGH', 'SCHOOL', 'TRANSFER', 'D75','YABC']
        city_agency_list = ['311','ACS','BIC','BOE','BPL','CCHR','CCRB','CUNY','DCA','DCAS','DCLA','DCP','DDC','DEP','DFTA', 'NYCHH',\
            'DHS','DOB','DOC','DOE','DOF','DOHMH','DOI','DOITT','DOP','DOR','DOT','DPR','DSNY','DVS','DYCD','EDC',\
            'FDNY','HPD','HRA','LAW','LPC','NYCEM','NYCHA','NYPD','NYPL','OATH','OCME','QPL','SBS','SCA','TLC', 'DORIS', 'COMMISSION',\
            "OFFICE", "DEPT", "CITY", "DISTRICT", "PRESIDENT", "BOARD", "COMM", "DEPARTMENT", "AGENCY", "LIBRARY", "SERVICE"]
        color_list = ['BK', 'BL', 'BG', 'BR', 'GL', 'GY', 'MR', 'OR', 'PK', 'PR', 'RD', 'TN', 'WH', 'YW', \
            'BLACK', 'BLUE', 'BEIGE', 'BROWN', 'GOLD', 'GRAY', 'MAROON', 'ORANGE', 'PINK', 'PURPLE', 'RED', 'TAN', 'WHITE', 'YELLOW']
        vehicle_type_list = ['FIRE', 'CONV', 'SEDN', 'SUBN', '4DSD', '2DSD', 'H/WH', 'ATV', 'MCY', 'H/IN', 'LOCO', 'RPLC',\
            'AMBU', 'P/SH', 'RBM', 'R/RD', 'RD/S', 'S/SP', 'SN/P', 'TRAV', 'MOBL', 'TR/E', 'T/CR', 'TR/C', 'SWT',\
            'W/DR', 'W/SR', 'FPM', 'MCC', 'EMVR', 'TRAC', 'DELV', 'DUMP', 'FLAT', 'PICK', 'STAK', 'TANK',\
            'REFG', 'TOW', 'VAN', 'UTIL', 'POLE', 'BOAT', 'H/TR', 'SEMI', 'TRLR', 'LTRL', 'LSVT', 'BUS', 'LIM',\
            'HRSE', 'TAXI', 'DCOM', 'CMIX', 'MOPD', 'MFH', 'SNOW', 'LSV']
        area_of_study_list = ["ARCHITECTURE", "SCIENCE", "ART", "TEACHING", "TEACHING", "BUSINESS", "COMMUNICATIONS", "COSMETOLOGY",\
            "ENGINEERING", "HUMANITIES", "TECHNOLOGY", "HEALTH", "ECONOMICS", "ENVIRONMENT", "ALGEBRA", "CHEMISTRY",\
            "ENGLISH", "MATH", "SOCIAL", "STUDIES", "SOCIAL STUDIES"]
        subject_in_school_list = ["ENGLISH", "SCIENCE", "MATH", "SOCIAL", "STUDIES", "SOCIAL STUDIES", "HISTORY", "CHEMISTRY", "PHYSICS"]
        park_playground_list = ["PARK", "PLAYGROUND", "CENTER", "GARDEN", "GARDENS", "SCHOOL", "RECREATION", "POOL", "FIELD"]
        address_list = ["ST", "STREET", "AVENUE", "AVE", "ROAD", "RD", "HIGHWAY", "HWY", "SQUARE", "SQ", "TRAIL", "TRL", "DRIVE", "DR", \
            "COURT", "CT", "PARK", "PARKWAY", "PKWY", "CIRCLE", "CIR", "BLVD", "BOULEVARD"]
        school_list = ['SCHOOL', 'ACAD ', 'ACADEMIC', 'ACADEMY', 'INSTITUTE', 'CENTER', 'MS ', 'PS ', 'IS ', 'JHS ', 'SCH ', 'SCH-', 'ELEMENTARY', " HS", "PREPARATORY"]
        location_type_list = ["BUILDING", "AIRPORT", "ATM", "BANK", "CLUB", "BRIDGE", "TERMINAL", "STORE", "OFFICE",\
            "STATION", "HOSPITAL", "RESIDENCE", "RESTAURANT", "TUNNEL", "PARK", "SHELTER", "MAILBOX", "SCHOOL", "STREET",\
            "TRANSIT", "STOP", "FACTORY"]
        car_make_list = ["ACUR", "ALFA", "AMGN", "AMER", "ASTO", "AUDI", "AUST", "AVTI", "AUTU", "BENT", "BERO", "BLUI",\
            "BMW", "BRIC", "BROC", "BSA", "BUIC", "CADI", "CHEC", "CHEV", "CHRY", "CITR", "DAEW", "DAIH", "DATS",\
            "DELO", "DESO", "DIAR", "DINA", "DIVC", "DODG", "DUCA", "EGIL", "EXCL", "FERR", "FIAT", "FORD", "FRHT",\
            "FWD", "GZL", "GMC", "GRUM", "HD", "HILL", "HINO", "HOND", "HUDS", "HYUN", "CHRY", "INFI", "INTL",\
            "ISU", "IVEC", "JAGU", "JENS", "AMER", "AMER", "KAWK", "KW", "KIA", "LADA", "LAMO", "LNCI", "LNDR",\
            "LEXS", "LINC", "LOTU", "MACK", "MASE", "MAYB", "MAZD", "MCIN", "MERZ", "MERC", "MERK", "MG", "MITS",\
            "MORG", "MORR", "MOGU", "NAVI", "NEOP", "NISS", "NORT", "OLDS", "OPEL", "ONTR", "OSHK", "PACK", "PANZ",\
            "PTRB", "PEUG", "PLYM","PONT", "PORS", "RELA", "RENA", "ROL", "SAA", "STRN", "SCAN", "SIM", "SIN",\
            "STLG", "STU", "STUZ", "SUBA", "SUNB", "SUZI", "THMS", "TOYT", "TRIU", "TVR", "UD", "VCTY", "VOLK",\
            "VOLV", "WSTR", "WHIT", "WHGM", "AMER", "YAMA", "YUGO"]
        school_name_list = ['PREP', 'COLLEGE', 'SCHOOL', 'ACAD ', 'ACADEMIC', 'ACADEMY', 'INSTITUTE', 'CENTER', 'MS ', \
            'PS ', 'IS ', 'JHS ', 'SCH ', 'SCH-', 'ELEMENTARY', " HS", "PREPARATORY"]
        college_name_list = ['UNIVERSITY', 'COLLEGE', 'INSTITUTE']
        city_list = ["NEW", "YORK","BUFFALO","ROCHESTER","YONKERS","SYRACUSE","ALBANY","ROCHELLE","MOUNT" ,"VERNON","SCHENECTADY",\
            "UTICA","WHITE PLAINS","HEMPSTEAD","TROY","NIAGARA FALLS","BINGHAMTON","FREEPORT","VALLEY STREAM","LONG BEACH","SPRING", "VALLEY","ROME",\
            "ITHACA","NORTH TONAWANDA","POUGHKEEPSIE","PORT CHESTER","JAMESTOWN","HARRISON","NEWBURGH","SARATOGA SPRINGS","MIDDLETOWN","GLEN", "COVE",\
            "ELMIRA","LINDENHURST","AUBURN","KIRYAS JOEL","ROCKVILLE CENTRE","OSSINING","PEEKSKILL","WATERTOWN","KINGSTON","GARDEN CITY","LOCKPORT",\
            "PLATTSBURGH","MAMARONECK","LYNBROOK","MINEOLA","CORTLAND","SCARSDALE","COHOES","LACKAWANNA","AMSTERDAM","MASSAPEQUA PARK","OSWEGO","RYE",\
            "FLORAL PARK","WESTBURY","DEPEW","KENMORE","GLOVERSVILLE","TONAWANDA","GLENS FALLS","MASTIC BEACH","BATAVIA","JOHNSON CITY","ONEONTA","BEACON",\
            "OLEAN","ENDICOTT","GENEVA","PATCHOGUE","HAVERSTRAW","BABYLON","TARRYTOWN","DUNKIRK","MOUNT KISCO","DOBBS FERRY","LAKE GROVE","FULTON","ONEIDA",\
            "WOODBURY","SUFFERN","CORNING","FREDONIA","OGDENSBURG","WEST HAVERSTRAW","GREAT NECK","SLEEPY HOLLOW","CANANDAIGUA","LANCASTER","MASSENA",\
            "WATERVLIET","NEW HYDE PARK","EAST ROCKAWAY","POTSDAM","AMITYVILLE","RYE BROOK","HAMBURG","FARMINGDALE","RENSSELAER","NEW SQUARE","AIRMONT","NEWARK",\
            "MONROE","MALVERNE","PORT JERVIS","CROTON-ON-HUDSON","JOHNSTOWN","BROCKPORT","GENESEO","CHESTNUT RIDGE","HORNELL","BRIARCLIFF MANOR","BALDWINSVILLE",\
            "HASTINGS-ON-HUDSON","PORT JEFFERSON","ILION","COLONIE","SCOTIA","HERKIMER","WILLISTON PARK","EAST HILLS","NORTHPORT","GREAT NECK PLAZA","PLEASANTVILLE",\
            "PELHAM","NEW PALTZ","NYACK","HUDSON FALLS","WARWICK","BAYVILLE","MANORHAVEN","CEDARHURST","WALDEN","LAWRENCE","NORTH HILLS","NORTH SYRACUSE","TUCKAHOE",\
            "IRVINGTON","NORWICH","EAST ROCHESTER","CANTON","BRONXVILLE","MONTICELLO","HORSEHEADS","SOLVAY","LARCHMONT","EAST AURORA","HUDSON","KASER","WESLEY HILLS","ALBION",\
            "NEW HEMPSTEAD","HILTON","WASHINGTONVILLE","ELMSFORD","MEDINA","WEBSTER","MALONE","FAIRPORT","PELHAM MANOR","BATH","SALAMANCA","WAPPINGERS FALLS","GOSHEN","KINGS POINT",\
            "SARANAC", "LAKE","WILLIAMSVILLE","BALLSTON SPA","SEA CLIFF","WATERLOO","ISLAND PARK","MECHANICVILLE","FLOWER HILL","PENN YAN","OLD WESTBURY","CHITTENANGO","ARDSLEY",\
            "LITTLE FALLS","MONTEBELLO","COBLESKILL","MONTGOMERY","CANASTOTA","DANSVILLE","MANLIUS","WELLSVILLE","SPRINGVILLE","CHESTER","LE ROY","HAMILTON","ALFRED","FAYETTEVILLE",\
            "LIBERTY","WAVERLY","OWEGO","ELLENVILLE","MENANDS","MAYBROOK","SPENCERPORT","ELMIRA HEIGHTS","HIGHLAND FALLS","SAUGERTIES"]
        neighborhood_list = []

        #s = "json/" + file + ".json"
        #with open(s, 'w') as f:
        #   json.dump(temp_output, f)

    #print(output)
    #with open('task2.json', 'w') as f:
        #json.dump(output, f)
