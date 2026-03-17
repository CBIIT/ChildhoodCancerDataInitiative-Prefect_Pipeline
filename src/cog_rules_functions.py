import pandas as pd
import re
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

# -----------------------------
# Transformation base class
# -----------------------------
class Transformation(ABC):
    def __init__(self, rule_row: pd.Series):
        self.rule = rule_row
        self.inputs = self._parse_inputs(rule_row.get("inputs"))

    @staticmethod
    def _parse_inputs(inputs):
        if pd.isna(inputs):
            return []
        return [c.strip() for c in str(inputs).split(",")]

    @abstractmethod
    def apply(self, row: pd.Series):
        pass


class Liftover(Transformation):
    def apply(self, row):
        if not self.inputs:
            return None
        return row.get(self.inputs[0])


class Concatenation(Transformation):
    def apply(self, row):
        values = [str(row.get(c)) for c in self.inputs if pd.notna(row.get(c))]
        if not values:
            return None
        return "_".join(values)


class Difference(Transformation):
    """Class for calculating the difference between two columns, with error handling for non-numeric and missing values"""
    def apply(self, row):
        if len(self.inputs) != 2:
            return None
        a, b = row.get(self.inputs[0]), row.get(self.inputs[1])
        if pd.isna(a) or pd.isna(b):
            return None
        try:
            a, b = int(a), int(b)
        except ValueError:
            return None
        return b - a

class Age_Event_Mapper(Transformation):
    def apply(self, row):
        if len(self.inputs) != 2:
            return None
        a, b = row.get(self.inputs[0]), row.get(self.inputs[1])
        if pd.isna(a) or pd.isna(b):
            return -999
        try:
            a, b = int(a), int(b)
        except ValueError:
            return -999
        return abs(a) + b

class Race_Ethnicity_Mapper(Transformation):
    """Class for transforming race and ethnicity columns into a single column"""
    def apply(self, row):
        if not self.inputs:
            return None
        a, b = row.get(self.inputs[0]), row.get(self.inputs[1])
        if pd.isna(a) or pd.isna(b):
            return None
        a = str(a).title().replace("Or", "or")
        b = str(b).title().replace("Or", "or")
        
        # if b == "Not Hispanic or Latino", replace as ""
        if b in ["Not Hispanic or Latino", "Unknown", "Not Reported"]:
            b = ""
        
        if b != "" and a in ["Unknown", "Not Reported"]:
            a = ""
        
        if a == "" and b == "":
            return "Not Reported"
        
        if b != "":
            return f"{a};{b}" if a != "" else b
        return a
        

class CNS_Tumor_Spatial_Extent_Mapper(Transformation):
    """Class for mapping CNS tumor spatial extent based on Chang staging system, with
    mapping to https://archive.datadictionary.nhs.uk/DD%20Release%20May%202024/data_elements/chang_staging_system_stage.html"""
    def apply(self, row):
        if not self.inputs:
            return None
        values = [str(row.get(c)) for c in self.inputs if pd.notna(row.get(c))]
        if not values:
            return "Not Reported"
        # Map specific combinations to standardized terms
        mapping_dict = {
            "Localized": "Local",
            "Metastatic": "Metastatic",
            "Distant": "Metastatic",
            "Not Answered": "Not Reported",
            "M0": "Local", 
            "M0 or M1": "Locoregional", 
            "M1": "Locoregional", 
            "M2": "Regional", 
            "M3": "Metastatic",
            "M4": "Metastatic"
            }
        
        mapped_values = []
        for v in values:
            mapped_values.append(mapping_dict.get(v, v))
        if mapped_values:
            return ";".join(set(mapped_values))
        return "Not Reported"

class Parse_List_Values(Transformation):
    """"Class for parsing from a list columns any non-null value and returning it as a concatenated string"""
    def apply(self, row):
        values = [str(row.get(c)) for c in self.inputs if pd.notna(row.get(c))]
        if not values:
            return None
        return ";".join(set(values))

class Default_Mapper(Transformation):
    """Class for auto-assigning a default value provided in the mapping file"""
    def apply(self, row):
        return self.rule.get("modifier_value")

class Diagnosis_Basis_Mapper(Transformation):
    """Class for mapping diagnosis basis to a standardized set of terms based on the provided mapping file;
    Takes a list of input columns, and if any of the columns have a non-null value, maps to a standardized term based on the mapping file;
    concatenates to a single, ; delimited value if multiple input columns have non-null values"""
    def apply(self, row):
        if not self.inputs:
            return None
        values = [str(row.get(c)).title() for c in self.inputs if pd.notna(row.get(c))]
        if not values:
            return None
        # replace any values taht contain str 'biopsy' with 'Pathology'
        values = ["Pathology" if "iopsy" in v else v for v in values]
        
        # mappings for additional values based on provided mapping file
        # if any of the values are in the mapping dict, replace with mapped value; if multiple values are in the mapping dict, concatenate mapped values and any unmapped values to a single, ; delimited string
        mapping_dict = {
            "Histology" : "Pathology",
            "Imaging" : "Pathology",
            "Other" : "Not Reported",
            "Tumor Marker" : "Molecular",
        }
        
        mapped_values = []
        for v in values:
            mapped_values.append(mapping_dict.get(v, v))
        return ";".join(set(mapped_values))

class Tumor_Grade_Mapper(Transformation):
    """Class for mapping tumor grade to a standardized set of terms based on the provided mapping file;
    given a list of input columns, if any of the columns have a non-null value, maps to a standardized term based on the mapping file"""
    
    def apply(self, row):
        if not self.inputs:
            return None
        values = [str(row.get(c)) for c in self.inputs if pd.notna(row.get(c))]
        if not values:
            return None
        # mappings for values based on provided mapping file
        # if any of the values are in the mapping dict, replace with mapped value; if multiple values are in the mapping dict, concatenate mapped values and any unmapped values to a single, ; delimited string
        mapping_dict = {
            "I" : "G1 Low Grade",
            "II" : "G2 Intermediate Grade",
            "III" : "G3 High Grade",
            "IV" : "G4 Anaplastic",
            "Grade I" : "G1 Low Grade",
            "Grade II" : "G2 Intermediate Grade",
            "Grade III" : "G3 High Grade",
            "Grade IV" : "G4 Anaplastic",
            "Not Applicable" : "Not Applicable",
            "Unknown" : "Unknown",
            "Unknown/Not applicable" : "Unknown",
        }
        
        mapped_values = []
        for v in values:
            mapped_values.append(mapping_dict.get(v, v))
        return ";".join(set(mapped_values))

class Substudy_Integrated_Dx_Mapper(Transformation):
    def apply(self, row):
        values = [str(row.get(c)) for c in self.inputs if pd.notna(row.get(c))]
        if not values:
            return None
        # remove any value in values that == 'Other' or 'Unknown' or 'Not Reported'
        values = [v for v in values if v not in ['Other', 'Unknown', 'Not Reported']]
        if not values:
            return "Not Reported"
        return ";".join(set(values))


class EFS_Status(Transformation):
    def apply(self, row):
        values = [str(row.get(c)) for c in self.inputs if pd.notna(row.get(c))]
        if not values:
            return None
        # remove any value in values that == 'Other' or 'Unknown' or 'Not Reported'
        values = [v for v in values if v not in ['Other', 'Unknown', 'Not Reported']]
        if values:
            return "Not Censored"
        else:
            return None
        
class EFS_Age(Transformation):
    def apply(self, row):
        values = [str(row.get(c)) for c in self.inputs if pd.notna(row.get(c))]
        if not values or len(values) != 2:
            return None
    
        # smallest value is birthdate
        try:            
            values = [int(v) for v in values]
        except ValueError:
            return None
        birth = min(values)
        event = max(values)
        return event - birth

class Follow_Up_Treatment_Response_Mapper(Transformation):
    def apply(self, row):
        values = [str(row.get(c)) for c in self.inputs]
        
        # zip values to keys based on order in mapping file
        # e.g. FOLLOW_UP.DZ_EXM_REP_IND_2, FOLLOW_UP.COMP_RESP_CONF_IND_3, FOLLOW_UP.DZ_REL_PROG_IND3 (evaluated, complete remission, progression)
        
        keys = ["FOLLOW_UP.FSTLNTXINIDXADM", "FOLLOW_UP.DZ_EXM_REP_IND_2", "FOLLOW_UP.COMP_RESP_CONF_IND_3", "FOLLOW_UP.DZ_REL_PROG_IND3"]
        
        values = {k:v for k,v in zip(keys, values)}
        
        # logic
        # complete response == yes, no progression, yes confirmed response       
        if values["FOLLOW_UP.FSTLNTXINIDXADM"] == "Yes":
            if values["FOLLOW_UP.DZ_EXM_REP_IND_2"] == "Yes":
                if values["FOLLOW_UP.COMP_RESP_CONF_IND_3"] == "Yes" and values["FOLLOW_UP.DZ_REL_PROG_IND3"] == "No":
                    return "Complete Remission"
                elif values["FOLLOW_UP.DZ_REL_PROG_IND3"] == "Yes":
                    return "Progressive Disease"
                else:
                    return "Unknown"
        
        else:   
            if values["FOLLOW_UP.COMP_RESP_CONF_IND_3"] == "Yes" and values["FOLLOW_UP.DZ_REL_PROG_IND3"] == "No":
                return "Complete Remission"
        
        return "Not Reported"
    
class Other_Treatment_Type_Mapper(Transformation):
    def apply(self, row):
        values = [str(row.get(c)) for c in self.inputs if pd.notna(row.get(c))]
        if not values:
            return None
        # remove any value in values that == 'Other' or 'Unknown' or 'Not Reported'
        values = [v for v in values if v not in ['Other', 'Unknown', 'Not Reported']]
        if not values:
            return "Not Reported"
        
        mapping_dict = {
            "Cord blood" : "Cord Blood Stem Cell Transplant",
            "Autologous PBSC" : "Autologous Peripheral Blood Stem Cell Transplant",
            "Autologous bone marrow" : "Autologous Bone Marrow Transplant",
            "Other" : "Other Stem Cell Transplant",
        }
        
        mapped_values = []
        for v in values:
            mapped_values.append(mapping_dict.get(v, v))
        return ";".join(set(mapped_values))
    
class Surgery_Type_Mapper(Transformation):
    def apply(self, row):
        values = [str(row.get(c)) for c in self.inputs]
        if not values:
            return None
        
        keys = ["ON_STUDY_DX.PR_PRSCAT_EXT","CNS_ON_STUDY_INITIAL_DIAGNOSIS.PR_PRSCAT_EXT_SURGRPT","ON_STUDY_DX_CNS.TUM_RES_EXT_TP","ADRENOCORTICAL_CARCINOMA.SX_EXC_PERF_IND2","ON_STUDY_DX_SOFT_TISSUE_SARCOMA.MGNSTPTMRCTN","ON_STUDY_DX_SOFT_TISSUE_SARCOMA.RGLMPNDEXCRSLTP","ON_STDY_AT_INITIAL_DIAGNOSIS.PR_PRSCAT_EXT_SURGRPT","ON_STUDY_DX_EWING_SARCOMA.SURG_MARG_RES_FNG_TP","ON_STUDY_DX_GERM_CELL_TUMOR.LNBX_SMPLG_IND2","ON_STUDY_DX_GERM_CELL_TUMOR.FRST_DX_SURG_IND","SURG_RESECTION_OSTEOSARCOMA.TUM_RES_EXT_TP","SURG_RESECTION_OSTEOSARCOMA.SURG_MARG_RES_FNG_TP","ON_STUDY_DX_SOFT_TISSUE_SARCOMA.SURG_RESECT_EXT_TP","SURGERY.SURG_RESECT_EXT_TP", "FSTLNTXINIDXADMCAT_A4"]
        
        values = {k:v for k,v in zip(keys, values)}
        
        if values['ADRENOCORTICAL_CARCINOMA.SX_EXC_PERF_IND2'] == "Yes":
            values['ADRENOCORTICAL_CARCINOMA.SX_EXC_PERF_IND2'] = "Resection"
        elif values['ADRENOCORTICAL_CARCINOMA.SX_EXC_PERF_IND2'] == "No":
            values['ADRENOCORTICAL_CARCINOMA.SX_EXC_PERF_IND2'] = "Not Reported"
        
        if values['ON_STUDY_DX_GERM_CELL_TUMOR.LNBX_SMPLG_IND2'] == 'Yes':
            values['ON_STUDY_DX_GERM_CELL_TUMOR.LNBX_SMPLG_IND2'] = "Biopsy"
        else:
            values['ON_STUDY_DX_GERM_CELL_TUMOR.LNBX_SMPLG_IND2'] = "Not Applicable"
        if values['ON_STUDY_DX_GERM_CELL_TUMOR.FRST_DX_SURG_IND'] == 'Yes':
            values['ON_STUDY_DX_GERM_CELL_TUMOR.FRST_DX_SURG_IND'] = "Resection"
        else:            
            values['ON_STUDY_DX_GERM_CELL_TUMOR.FRST_DX_SURG_IND'] = "Not Applicable" #post hoc filtering of NAs that map to surgery/biopsy == No
            
        mapping_dict = {
            'nan' : 'Not Applicable',
            'Negative': 'Resection', 
            'Incomplete Resection': 'Resection', 
            'Gross Total Resection': 'Resection', 
            'NA (no surgical intervention)': 'Not Applicable',
            'Extensive subtotal resection': 'Resection', 
            'Not Applicable': 'Not Applicable', 
            'Subtotal resection with bulk residual disease (R2)': 'Resection', 
            'Less than gross total resection': 'Resection', 
            'Gross total resection': 'Resection', 
            'No Procedure': 'Not Applicable', 
            'Other': 'Resection', 
            'Not applicable': 'Not Applicable', 
            'Partial resection': 'Resection', 
            'Unknown': 'Resection', # unknown margins indicate resection performed
            'Gross total resection with no microscopic residual disease': 'Resection', 
            'Biopsy Only': 'Biopsy', 
            'Biopsy only': 'Biopsy', 
            'Subtotal resection with bulk residual disease': 'Resection', 
            'Unknown/Unavailable': 'Resection', 
            'Positive': 'Resection', 
            'Subtotal resection': 'Resection', 
            'Gross total resection with microscopic residual disease': 'Resection',
            'Surgery' : 'Not Reported'
            }
        
        
        mapped_values = []
        for k,v in values.items():
            mapped_values.append(mapping_dict.get(v, v))
        mapped_values_parse = list(set([i for i in mapped_values if i != 'Not Applicable']))
        
        return ";".join(mapped_values_parse) if mapped_values_parse else None
        
class Resection_Margin_Mapper(Transformation):
    def apply(self, row):
        values = [str(row.get(c)) for c in self.inputs]
        if not values:
            return None
        
        keys = ["ON_STUDY_DX.PR_PRSCAT_EXT","CNS_ON_STUDY_INITIAL_DIAGNOSIS.PR_PRSCAT_EXT_SURGRPT","ON_STUDY_DX_CNS.TUM_RES_EXT_TP","ADRENOCORTICAL_CARCINOMA.SX_EXC_PERF_IND2","ON_STUDY_DX_SOFT_TISSUE_SARCOMA.MGNSTPTMRCTN","ON_STUDY_DX_SOFT_TISSUE_SARCOMA.RGLMPNDEXCRSLTP","ON_STDY_AT_INITIAL_DIAGNOSIS.PR_PRSCAT_EXT_SURGRPT","ON_STUDY_DX_EWING_SARCOMA.SURG_MARG_RES_FNG_TP","ON_STUDY_DX_GERM_CELL_TUMOR.LNBX_SMPLG_IND2","ON_STUDY_DX_GERM_CELL_TUMOR.FRST_DX_SURG_IND","SURG_RESECTION_OSTEOSARCOMA.TUM_RES_EXT_TP","SURG_RESECTION_OSTEOSARCOMA.SURG_MARG_RES_FNG_TP","ON_STUDY_DX_SOFT_TISSUE_SARCOMA.SURG_RESECT_EXT_TP","SURGERY.SURG_RESECT_EXT_TP"]
        
        values = {k:v for k,v in zip(keys, values)}
        
        if values['ADRENOCORTICAL_CARCINOMA.SX_EXC_PERF_IND2'] == "Yes":
            values['ADRENOCORTICAL_CARCINOMA.SX_EXC_PERF_IND2'] = "R0, All Margins Pathologically Negative"
        elif values['ADRENOCORTICAL_CARCINOMA.SX_EXC_PERF_IND2'] == "No":
            values['ADRENOCORTICAL_CARCINOMA.SX_EXC_PERF_IND2'] = "RX, Presence of Residual Disease Cannot Be Assessed"
        
        # if ON_STUDY_DX_SOFT_TISSUE_SARCOMA.MGNSTPTMRCTN and ON_STUDY_DX_SOFT_TISSUE_SARCOMA.RGLMPNDEXCRSLTP both non-null/'nan' values, use MGNSTPTMRCTN value for margin status, as it contains more specific graded margin status (R0, R1, R2), while RGLMPNDEXCRSLTP contains more non-specific positive vs negative margin status
        if pd.notna(values['ON_STUDY_DX_SOFT_TISSUE_SARCOMA.MGNSTPTMRCTN']) and str(values['ON_STUDY_DX_SOFT_TISSUE_SARCOMA.MGNSTPTMRCTN']).lower() != 'nan':
            values['ON_STUDY_DX_SOFT_TISSUE_SARCOMA.RGLMPNDEXCRSLTP'] = values['ON_STUDY_DX_SOFT_TISSUE_SARCOMA.MGNSTPTMRCTN']
        
        
        mapping_dict = {
            'nan' : 'Not Applicable',
            'Negative': "R0, All Margins Pathologically Negative",
            'Incomplete Resection': 'R2, Macroscopically Positive Margins or Gross Residual Disease', 
            'Gross Total Resection': "R0, All Margins Pathologically Negative", 
            'NA (no surgical intervention)': 'Not Applicable',
            'Extensive subtotal resection': "R2, Macroscopically Positive Margins or Gross Residual Disease", 
            'Not Applicable': 'Not Applicable', 
            'Subtotal resection with bulk residual disease (R2)': "R2, Macroscopically Positive Margins or Gross Residual Disease", 
            'Less than gross total resection': "R2, Macroscopically Positive Margins or Gross Residual Disease", 
            'Gross total resection': "R0, All Margins Pathologically Negative", 
            'No Procedure': 'Not Applicable', 
            'Other': 'Not Applicable', 
            'Not applicable': 'Not Applicable', 
            'Partial resection': "R2, Macroscopically Positive Margins or Gross Residual Disease", 
            'Unknown': "RX, Presence of Residual Disease Cannot Be Assessed", # unknown margins indicate resection performed
            'Gross total resection with no microscopic residual disease': "R0, All Margins Pathologically Negative", 
            'Biopsy Only': 'Not Applicable', 
            'Biopsy only': 'Not Applicable', 
            'Subtotal resection with bulk residual disease': "R2, Macroscopically Positive Margins or Gross Residual Disease", 
            'Unknown/Unavailable': "RX, Presence of Residual Disease Cannot Be Assessed", 
            'Positive': 'Positive Margins, NOS', 
            'Subtotal resection': "R2, Macroscopically Positive Margins or Gross Residual Disease", 
            'Gross total resection with microscopic residual disease': "R1, Microscopically Positive Margins"
            }
        
        mapped_values = []
        for k,v in values.items():
            mapped_values.append(mapping_dict.get(v, v))
        mapped_values_parse = list(set([i for i in mapped_values if i != 'Not Applicable']))
        
        
        if len(mapped_values_parse) > 1:
            if 'RX, Presence of Residual Disease Cannot Be Assessed' in mapped_values_parse:
                # remove 'RX, Presence of Residual Disease Cannot Be Assessed' if other values present, as this indicates unknown margin status in the context of known resection
                mapped_values_parse.remove('RX, Presence of Residual Disease Cannot Be Assessed')
            if 'Positive Margins, NOS' in mapped_values_parse:
                # remove Positive Margins to favor known graded margin status, as Positive Margins is non-specific and can apply to both R1 and R2 resections
                mapped_values_parse.remove('Positive Margins, NOS')
                
        
        return ";".join(mapped_values_parse) if mapped_values_parse else None
    
class CNS_Chemo_ID_Mapper(Transformation):
    def apply(self, row):
        values = [str(row.get(c)) for c in self.inputs if pd.notna(row.get(c))]
        if not values:
            return None
        
        # remove any value in values that == 'Other' or 'Unknown' or 'Not Reported'
        values = [v for v in values if v not in ['Other', 'Unknown', 'Not Reported']]
        
        if not values or len(values) < 2:
            return None
        
        #regex replace brand names for drugs
        generic_values = []
        for v in values:
            generic_values.append(re.sub("\s\([A-Za-z0-9, \-\.]+\)?", '', v))
        
        mapping_dict = {
            '13-cis- retinoic acid': 'treatment_chemo_pharma',
            'Bevacizumab': 'treatment_chemo_immuno',
            'Bleomycin': 'treatment_chemo_chemo',
            'Busulfan': 'treatment_chemo_chemo',
            'Carboplatin': 'treatment_chemo_chemo',
            'Carmustine': 'treatment_chemo_chemo',
            'Cetuximab': 'treatment_chemo_immuno',
            'Cisplatin': 'treatment_chemo_chemo',
            'Crizotinib': 'treatment_chemo_tmt',
            'Cyclophosphamide': 'treatment_chemo_chemo',
            'Cytarabine': 'treatment_chemo_chemo',
            'Dacarbazine': 'treatment_chemo_chemo',
            'Dactinomycin': 'treatment_chemo_chemo',
            'Dexamethasone': 'treatment_chemo_chemo',
            'Dinutuximab': 'treatment_chemo_immuno',
            'Docetaxel': 'treatment_chemo_chemo',
            'Doxorubicin': 'treatment_chemo_chemo',
            'Eribulin': 'treatment_chemo_chemo',
            'Erlotinib': 'treatment_chemo_tmt',
            'Etoposide': 'treatment_chemo_chemo',
            'Fluorouracil': 'treatment_chemo_chemo',
            'Ganitumab': 'treatment_chemo_immuno',
            'Gefitinib': 'treatment_chemo_tmt',
            'Gemcitabine': 'treatment_chemo_chemo',
            'Ifosfamide': 'treatment_chemo_chemo',
            'Interleukin 2': 'treatment_chemo_immuno',
            'Irinotecan': 'treatment_chemo_chemo',
            'Lapatinib': 'treatment_chemo_tmt',
            'Lenalidomide': 'treatment_chemo_immuno',
            'Lomustine': 'treatment_chemo_chemo',
            'Melphalan': 'treatment_chemo_chemo',
            'Methotrexate': 'treatment_chemo_chemo',
            'Mitomycin C': 'treatment_chemo_chemo',
            'Oxaliplatin': 'treatment_chemo_chemo',
            'Paclitaxel': 'treatment_chemo_chemo',
            'Pazopanib': 'treatment_chemo_tmt',
            'Prednisone': 'treatment_chemo_chemo',
            'Sirolimus': 'treatment_chemo_tmt',
            'Sorafenib': 'treatment_chemo_tmt',
            'Sunitinib': 'treatment_chemo_tmt',
            'Temozolomide': 'treatment_chemo_chemo',
            'Temsirolimus': 'treatment_chemo_tmt',
            'Topotecan': 'treatment_chemo_chemo',
            'Vandetanib': 'treatment_chemo_tmt',
            'Vinblastine': 'treatment_chemo_chemo',
            'Vincristine': 'treatment_chemo_chemo',
            'Vinorelbine': 'treatment_chemo_chemo',
            'Vorinostat': 'treatment_chemo_pharma',
            'Nivolumab': 'treatment_chemo_immuno',
            'Pembrolizumab': 'treatment_chemo_immuno',
            'Dabrafenib': 'treatment_chemo_tmt',
            'Ivosidenib': 'treatment_chemo_tmt',
            'Larotrectinib': 'treatment_chemo_tmt',
            'Mirdametinib': 'treatment_chemo_tmt',
            'Ribociclib': 'treatment_chemo_tmt',
            'Selumetinib': 'treatment_chemo_tmt',
            'Tazemetostat': 'treatment_chemo_tmt',
            'Tovorafenib': 'treatment_chemo_tmt',
            'Trametinib': 'treatment_chemo_tmt',
            'Selinexor': 'treatment_chemo_pharma',
            'ONC201': 'treatment_chemo_pharma'
            }
        
        mapped_values = []
        for v in generic_values:
            mapped_values.append(mapping_dict.get(v, v))
        
        return "_".join(set(mapped_values))
    
class CNS_Chemo_Type_Mapper(Transformation):
    def apply(self, row):
        values = [str(row.get(c)) for c in self.inputs if pd.notna(row.get(c))]
        if not values:
            return None
        
        # remove any value in values that == 'Other' or 'Unknown' or 'Not Reported'
        values = [v for v in values if v not in ['Other', 'Unknown', 'Not Reported']]
        if not values:
            return None
        
        if len(values) > 1:
            return None
        
        value = values[0]
        
        #regex replace brand names for drugs
        generic_value = re.sub("\s\([A-Za-z0-9, \-\.]+\)?", '', value)
        
        mapping_dict = {
            '13-cis- retinoic acid': 'Pharmacotherapy',
            'Bevacizumab': 'Immunotherapy',
            'Bleomycin': 'Chemotherapy',
            'Busulfan': 'Chemotherapy',
            'Carboplatin': 'Chemotherapy',
            'Carmustine': 'Chemotherapy',
            'Cetuximab': 'Immunotherapy',
            'Cisplatin': 'Chemotherapy',
            'Crizotinib': 'Targeted Molecular Therapy',
            'Cyclophosphamide': 'Chemotherapy',
            'Cytarabine': 'Chemotherapy',
            'Dacarbazine': 'Chemotherapy',
            'Dactinomycin': 'Chemotherapy',
            'Dexamethasone': 'Chemotherapy',
            'Dinutuximab': 'Immunotherapy',
            'Docetaxel': 'Chemotherapy',
            'Doxorubicin': 'Chemotherapy',
            'Eribulin': 'Chemotherapy',
            'Erlotinib': 'Targeted Molecular Therapy',
            'Etoposide': 'Chemotherapy',
            'Fluorouracil': 'Chemotherapy',
            'Ganitumab': 'Immunotherapy',
            'Gefitinib': 'Targeted Molecular Therapy',
            'Gemcitabine': 'Chemotherapy',
            'Ifosfamide': 'Chemotherapy',
            'Interleukin 2': 'Immunotherapy',
            'Irinotecan': 'Chemotherapy',
            'Lapatinib': 'Targeted Molecular Therapy',
            'Lenalidomide': 'Immunotherapy',
            'Lomustine': 'Chemotherapy',
            'Melphalan': 'Chemotherapy',
            'Methotrexate': 'Chemotherapy',
            'Mitomycin C': 'Chemotherapy',
            'Oxaliplatin': 'Chemotherapy',
            'Paclitaxel': 'Chemotherapy',
            'Pazopanib': 'Targeted Molecular Therapy',
            'Prednisone': 'Chemotherapy',
            'Sirolimus': 'Targeted Molecular Therapy',
            'Sorafenib': 'Targeted Molecular Therapy',
            'Sunitinib': 'Targeted Molecular Therapy',
            'Temozolomide': 'Chemotherapy',
            'Temsirolimus': 'Targeted Molecular Therapy',
            'Topotecan': 'Chemotherapy',
            'Vandetanib': 'Targeted Molecular Therapy',
            'Vinblastine': 'Chemotherapy',
            'Vincristine': 'Chemotherapy',
            'Vinorelbine': 'Chemotherapy',
            'Vorinostat': 'Pharmacotherapy',
            'Nivolumab': 'Immunotherapy',
            'Pembrolizumab': 'Immunotherapy',
            'Dabrafenib': 'Targeted Molecular Therapy',
            'Ivosidenib': 'Targeted Molecular Therapy',
            'Larotrectinib': 'Targeted Molecular Therapy',
            'Mirdametinib': 'Targeted Molecular Therapy',
            'Ribociclib': 'Targeted Molecular Therapy',
            'Selumetinib': 'Targeted Molecular Therapy',
            'Tazemetostat': 'Targeted Molecular Therapy',
            'Tovorafenib': 'Targeted Molecular Therapy',
            'Trametinib': 'Targeted Molecular Therapy',
            'Selinexor': 'Pharmacotherapy',
            'ONC201': 'Pharmacotherapy'
            }
        
        mapped_value = mapping_dict.get(generic_value, generic_value)
        
        return mapped_value

TRANSFORM_REGISTRY = {
    "liftover": Liftover,
    "concatenation": Concatenation,
    "difference": Difference,
    "race_eth" : Race_Ethnicity_Mapper,
    "default": Default_Mapper,
    "tumor_spatial_extent_parse": CNS_Tumor_Spatial_Extent_Mapper,
    "parse" : Parse_List_Values,
    "dx_basis" : Diagnosis_Basis_Mapper,
    "tumor_grade" : Tumor_Grade_Mapper,
    "substudy_dx" : Substudy_Integrated_Dx_Mapper,
    "age_event" : Age_Event_Mapper,
    "efs_status" : EFS_Status,
    "efs_age" : EFS_Age,
    "follow_up_treat_response": Follow_Up_Treatment_Response_Mapper,
    "other_treatment_type": Other_Treatment_Type_Mapper,
    "surgery_type" : Surgery_Type_Mapper,
    "resection_margin_status" : Resection_Margin_Mapper,
    "cns_chemo_id" : CNS_Chemo_ID_Mapper,
    "cns_chemo_type" : CNS_Chemo_Type_Mapper,
    
}


# -----------------------------
# Modifier handling
# -----------------------------
def apply_modifier(value, modifiers, modifier_values):
    if value is None or pd.isna(value):
        return value
    
    if pd.isna(modifiers) or pd.isna(modifier_values):
        return value
    
    # zip modifiers and modifier values together and apply in sequence
    for modifier, modifier_value in zip(str(modifiers).split(","), str(modifier_values).split("#")):
        if modifier == "suffix":
            value = f"{value}_{modifier_value}"
        if modifier == "prefix":
            value = f"{modifier_value}_{value}"
        if modifier == "default":
            value = modifier_value
        if modifier == "priority":
            # select the n number of elements speicifed in modifier_value (e.g. "priority:2") 
            #  from a ; delimited list of values, based on a priority order specified in the mapping file (e.g. "High;Medium;Low")
            priority = int(modifier_value)
            value_list = str(value).split(";")
            if len(value_list) <= priority:
                continue
            value = ";".join(value_list[:priority])
        if modifier == "regex_replace":
            pat_match = modifier_value.split("%")[0]
            pat_replace = modifier_value.split("%")[1]
            value = re.sub(pat_match, pat_replace, str(value)).strip(pat_replace)
            # replace any 2+ instances of replace value with a single instance
            value = re.sub(f"{re.escape(pat_replace)}{{2,}}", pat_replace, value)
        if modifier == "regex_remove":
            value = re.sub(modifier_value, "", str(value))

    return value


# -----------------------------
# Rule
# -----------------------------
class Rule:
    def __init__(self, rule_row: pd.Series):
        self.node = rule_row["node"]
        self.mode = rule_row["mode"]
        self.property = rule_row["property"]

        self.core = bool(rule_row.get("core")) if not pd.isna(rule_row.get("core")) else False

        self.required_default = rule_row.get("required_default")

        fn = rule_row.get("function")
        if pd.isna(fn):
            raise ValueError(f"Missing function for {self.node}.{self.property}")

        self.function = str(fn).strip().lower()
        if self.function not in TRANSFORM_REGISTRY:
            raise ValueError(f"Unsupported function: {self.function}")

        self.transform = TRANSFORM_REGISTRY[self.function](rule_row)

        self.modifier = rule_row.get("modifier")
        self.modifier_value = rule_row.get("modifier_value")

    def has_any_input(self, row: pd.Series) -> bool:
        for c in self.transform.inputs:
            if pd.notna(row.get(c)):
                return True
        return False

    def apply(self, row: pd.Series):
        value = self.transform.apply(row)
        value = apply_modifier(value, self.modifier, self.modifier_value)

        if (value is None or pd.isna(value) or value == "" or str(value).upper() == "NA") and pd.notna(self.required_default):
            return self.required_default

        return value


# -----------------------------
# Mode group
# -----------------------------
class ModeGroup:
    def __init__(self, node: str, mode: str, rules: List[Rule]):
        self.node = node
        self.mode = mode
        self.rules = rules

        self.core_rules = [r for r in rules if r.core]

    def evaluate_row(self, row: pd.Series) -> Optional[Dict]:
        # Core gating
        if self.core_rules:
            if not any(r.has_any_input(row) for r in self.core_rules):
                return None

        output = {}
        for rule in self.rules:
            output[rule.property] = rule.apply(row)

        return output


# -----------------------------
# Engine
# -----------------------------
class TransformerEngine:
    def __init__(self, rules_df: pd.DataFrame):
        self.mode_groups = self._build_mode_groups(rules_df)

    @staticmethod
    def _build_mode_groups(rules_df):
        groups = {}

        for _, row in rules_df.iterrows():
            rule = Rule(row)
            key = (rule.node, rule.mode)
            groups.setdefault(key, []).append(rule)

        return {
            key: ModeGroup(key[0], key[1], rules)
            for key, rules in groups.items()
        }

    def transform(self, input_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        node_rows: Dict[str, List[Dict]] = {}

        for _, row in input_df.iterrows():
            for (node, _mode), group in self.mode_groups.items():
                result = group.evaluate_row(row)
                if result is not None:
                    node_rows.setdefault(node, []).append(result)

        return {
            node: pd.DataFrame(rows)
            for node, rows in node_rows.items()
        }