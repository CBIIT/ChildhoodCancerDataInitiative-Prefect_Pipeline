import pandas as pd
import re
import os
from abc import ABC, abstractmethod
from typing import Dict, List, Optional


def load_mapping(file_path, key_col, value_col) -> Dict:
    """utility func to load in mapping files as dicts for use in transformation functions"""
    mapping = {}
    
    # Validate file exists before attempting to read
    if not os.path.isfile(file_path):
        print(f"Warning: Mapping file not found: {file_path}")
        # logger.warning(f"Mapping file not found: {file_path}")
        return mapping
    
    try:
        df = pd.read_csv(file_path, sep="\t")
        
        # Validate required columns exist
        if key_col not in df.columns:
            print(f"Error: Key column '{key_col}' not found in {file_path}")
            return mapping
        if value_col not in df.columns:
            print(f"Error: Value column '{value_col}' not found in {file_path}")
            return mapping
            
        map_df = df[[key_col, value_col]].dropna().drop_duplicates()
        mapping = map_df.set_index(key_col)[value_col].to_dict()
        print(f"Successfully loaded {len(mapping)} mappings from {file_path}")
        
    except Exception as e:
        raise ValueError(f"Error loading mapping file {file_path}: {e}")
    return mapping


class MappingManager:
    """Lazy loading manager for mapping dictionaries with caching."""
    
    _cache = {}
    _mapping_configs = {
        'PRIMARY_SITE': ("docs/mci_gdc_anatomic_site_map.txt", "anatomic_site", "primary_site"),
        'TISSUE_ORGAN_ORIGIN': ("docs/mci_gdc_anatomic_site_map.txt", "anatomic_site", "tissue_or_organ_of_origin"),
        'DISEASE_TYPE': ("docs/mci_gdc_disease_type_map.txt", "diagnosis_category", "disease_type"),
        'MORPHOLOGY': ("docs/mci_gdc_primary_diagnosis_map.txt", "diagnosis", "morphology"),
        'PRIMARY_DIAGNOSIS': ("docs/mci_gdc_primary_diagnosis_map.txt", "diagnosis", "primary_diagnosis"),
    }
    
    @classmethod
    def get_mapping(cls, mapping_name: str) -> Dict:
        """Get a mapping dictionary, loading it if not already cached."""
        if mapping_name not in cls._cache:
            if mapping_name not in cls._mapping_configs:
                raise ValueError(f"Unknown mapping: {mapping_name}")
            
            file_path, key_col, value_col = cls._mapping_configs[mapping_name]
            cls._cache[mapping_name] = load_mapping(file_path, key_col, value_col)
        
        return cls._cache[mapping_name]
    
    @classmethod
    def clear_cache(cls):
        """Clear all cached mappings (useful for testing or reloading)."""
        cls._cache.clear()


# Backwards compatibility - these will now lazy load on first access
def _get_primary_site():
    return MappingManager.get_mapping('PRIMARY_SITE')

def _get_tissue_organ_origin():
    return MappingManager.get_mapping('TISSUE_ORGAN_ORIGIN')

def _get_disease_type():
    return MappingManager.get_mapping('DISEASE_TYPE')

def _get_morphology():
    return MappingManager.get_mapping('MORPHOLOGY')

def _get_primary_diagnosis():
    return MappingManager.get_mapping('PRIMARY_DIAGNOSIS')


# -----------------------------
# Transformation base class
# -----------------------------
class Transformation(ABC):
    def __init__(self, rule_row: pd.Series):
        self.rule = rule_row
        self.inputs = self._parse_inputs(rule_row.get("source_property"))

    @staticmethod
    def _parse_inputs(inputs):
        if pd.isna(inputs):
            return []
        return [c.strip() for c in str(inputs).split(",")]

    @abstractmethod
    def apply(self, row: pd.Series):
        pass


# -----------------------------
# Transformation functions classes
# -----------------------------


class Liftover(Transformation):
    """Class for direct mapping of a single input column to an output column without transformation"""

    def apply(self, row):
        if not self.inputs:
            return None
        return row.get(self.inputs[0])


class Concatenation(Transformation):
    """Class for concatenating values from multiple input columns into a single output column, with handling for missing values"""

    def apply(self, row):
        values = [str(row.get(c)) for c in self.inputs if pd.notna(row.get(c))]
        if not values:
            return None
        return "_".join(values)


class Race_Mapper(Transformation):
    """Class for transforming race values to match GDC standards"""

    def apply(self, row):
        if not self.inputs:
            return "Unknown"

        value = (
            str(row.get(self.inputs[0])) if pd.notna(row.get(self.inputs[0])) else ""
        )
        if not value:
            return "Unknown"

        # remove [Not] hispanic or latino from race
        values = value.split(";") if ";" in value else [value]
        values = [
            v.lower().strip()
            for v in values
            if "hispanic" not in v.lower() and "latino" not in v.lower()
        ]

        return values[0] if values else "Unknown"


class Ethnicity_Mapper(Transformation):
    """Class for transforming ethnicity values to match GDC standards"""

    def apply(self, row):
        if not self.inputs:
            return "not reported"

        value = (
            str(row.get(self.inputs[0])) if pd.notna(row.get(self.inputs[0])) else ""
        )
        if not value:
            return "not reported"

        # extract [Not] hispanic or latino from race
        values = value.split(";") if ";" in value else [value]
        values = [
            v.lower().strip()
            for v in values
            if "hispanic" in v.lower() or "latino" in v.lower()
        ]

        return values[0] if values else "not reported"


class Sex_Mapper(Transformation):
    """Class for transforming sex_at_birth values to match GDC standards"""

    def apply(self, row):
        if not self.inputs:
            return "unknown"
        value = (
            str(row.get(self.inputs[0])) if pd.notna(row.get(self.inputs[0])) else ""
        )
        if not value:
            return "unknown"
        if value.lower() in ["male", "m"]:
            return "male"
        if value.lower() in ["female", "f"]:
            return "female"
        return "unknown"


class Default_Mapper(Transformation):
    """Class for auto-assigning a default value provided in the mapping file"""

    def apply(self, row):
        return self.rule.get("modifier_value")


class WXS_RG_ID_Parse(Transformation):
    """Class for parsing read group ID from file name"""

    def apply(self, row):
        values = [str(row.get(c)) for c in self.inputs if pd.notna(row.get(c))]
        sample_id = values[0] if values else None
        file_name = values[1] if len(values) > 1 else None
        if not sample_id or not file_name:
            return None
        pattern = r"^(?:[^_]+_){3}([^_]+(?:_[^_]+)*)_R\d+_\d+"
        match = re.search(pattern, file_name)
        if match:
            return f"{sample_id}_rg_{match.group(1)}"
        return None


class Read_Pair_Parse(Transformation):
    """Class for parsing read pair from file name"""

    def apply(self, row):
        value = [str(row.get(c)) for c in self.inputs if pd.notna(row.get(c))][0]
        pattern = r"_(R\d)_\d+"
        match = re.search(pattern, value)
        if match:
            return match.group(1)
        return None


class Channel_Color(Transformation):
    """Class for assigning channel color based on file name pattern for methylation array IDAT files"""

    def apply(self, row):
        value = [str(row.get(c)) for c in self.inputs if pd.notna(row.get(c))][0]
        pattern = r"_(Grn|Red).idat"
        match = re.search(pattern, value)
        if match:
            if match.group(1).lower() == "grn":
                return "Green"
            if match.group(1).lower() == "red":
                return "Red"
        return None


class Specimen_Mapper(Transformation):
    """Class for mapping specimen type to match GDC standards"""

    def apply(self, row):
        if not self.inputs:
            return None
        value = (
            str(row.get(self.inputs[0])) if pd.notna(row.get(self.inputs[0])) else None
        )
        if not value:
            return None

        # map specimen type to GDC standards
        if "tumor" in value.lower():
            return "Solid Tissue"
        if "normal" in value.lower():
            return "Peripheral Whole Blood"
        return None


class Tumor_Desc_Mapper(Transformation):
    """Class for mapping tumor descriptor to match GDC standards"""

    def apply(self, row):
        if not self.inputs:
            return None
        values = [str(row.get(c)) for c in self.inputs if pd.notna(row.get(c))]
        tumor_spatial = values[0] if values else None
        sample_type = values[1] if len(values) > 1 else None
        if not values:
            return None

        # map tumor descriptor to GDC standards
        if "local" in tumor_spatial.lower():
            return "Primary"
        if "metastatic" in tumor_spatial.lower():
            return "Metastatic"
        if (
            "not reported" in tumor_spatial.lower()
            or "unknown" in tumor_spatial.lower()
        ):
            if sample_type.lower() in ["solid tissue", "tumor"]:
                return "Primary"
            if sample_type.lower() in ["peripheral whole blood", "normal"]:
                return "Not Applicable"
        return "Not Reported"


class Disease_Type_Mapper(Transformation):
    """Class for mapping disease type to match GDC standards"""

    def apply(self, row):
        if not self.inputs:
            return None
        value = (
            str(row.get(self.inputs[0])) if pd.notna(row.get(self.inputs[0])) else None
        )

        # strip whitespace and lowercase for mapping
        value = value.strip() if value else None

        if not value:
            return None

        # strip out unwanted entries
        unmatched = [
            "Other Solid Tumors",
            "Other CNS",
            "Other Hematopoietic Neoplasms",
            "Low-grade Gliomas",
            "Other Brain Tumors",
        ]

        values = [v.strip() for v in value.split(";")] if ";" in value else [value]
        values = [v for v in values if v not in unmatched]

        mapped_vals = set([_get_disease_type().get(v, "Not Mapped") for v in values])
        if len(mapped_vals) == 1:
            return mapped_vals.pop()
        else:
            if "Not Mapped" in mapped_vals:
                mapped_vals.remove("Not Mapped")
            return ";".join(mapped_vals) if mapped_vals else "Not Mapped"


class Primary_Site_Mapper(Transformation):
    """Class for mapping primary site to match GDC standards"""

    def apply(self, row):
        if not self.inputs:
            return None
        value = (
            str(row.get(self.inputs[0])) if pd.notna(row.get(self.inputs[0])) else None
        )

        # strip whitespace and lowercase for mapping
        value = value.strip() if value else None

        if not value:
            return None

        # return PRIMARY_SITE.get(value, "Not Mapped")
        values = [v.strip() for v in value.split(";")] if ";" in value else [value]
        mapped_vals = set([_get_primary_site().get(v, "Not Mapped") for v in values])
        if len(mapped_vals) == 1:
            return mapped_vals.pop()
        else:
            if "Not Mapped" in mapped_vals:
                mapped_vals.remove("Not Mapped")
            return ";".join(mapped_vals) if mapped_vals else "Not Mapped"


class Tissue_Mapper(Transformation):
    """Class for mapping tissue or organ of origin to match GDC standards"""

    def apply(self, row):
        if not self.inputs:
            return None
        value = (
            str(row.get(self.inputs[0])) if pd.notna(row.get(self.inputs[0])) else None
        )

        # strip whitespace and lowercase for mapping
        value = value.strip() if value else None

        if not value:
            return None

        values = [v.strip() for v in value.split(";")] if ";" in value else [value]
        mapped_vals = set([_get_tissue_organ_origin().get(v, "Not Mapped") for v in values])
        if len(mapped_vals) == 1:
            return mapped_vals.pop()
        else:
            if "Not Mapped" in mapped_vals:
                mapped_vals.remove("Not Mapped")
            return ";".join(mapped_vals) if mapped_vals else "Not Mapped"


class Morphology_Mapper(Transformation):
    """Class for mapping morphology to match GDC standards"""

    def apply(self, row):
        if not self.inputs:
            return None
        value = (
            str(row.get(self.inputs[0])) if pd.notna(row.get(self.inputs[0])) else None
        )

        # strip whitespace and lowercase for mapping
        value = value.strip() if value else None

        if not value:
            return None

        mapped_val = _get_morphology().get(value, "Not Mapped")
        return mapped_val if mapped_val != "Not Mapped" else None


class Prim_Dx_Mapper(Transformation):
    """Class for mapping primary diagnosis to match GDC standards"""

    def apply(self, row):
        if not self.inputs:
            return None
        value = (
            str(row.get(self.inputs[0])) if pd.notna(row.get(self.inputs[0])) else None
        )

        # strip whitespace and lowercase for mapping
        value = value.strip() if value else None

        if not value:
            return None

        mapped_val = _get_primary_diagnosis().get(value, "Not Mapped")
        return mapped_val if mapped_val != "Not Mapped" else None


TRANSFORM_REGISTRY = {
    "liftover": Liftover,
    "concatenation": Concatenation,
    "race": Race_Mapper,
    "eth": Ethnicity_Mapper,
    "sex": Sex_Mapper,
    "default": Default_Mapper,
    "wxs_rg_id_parse": WXS_RG_ID_Parse,
    "read_pair_parse": Read_Pair_Parse,
    "disease_type_mapper": Disease_Type_Mapper,
    "primary_site_mapper": Primary_Site_Mapper,
    "prim_dx_mapper": Prim_Dx_Mapper,
    "morphology_mapper": Morphology_Mapper,
    "tissue_mapper": Tissue_Mapper,
    "specimen_mapper": Specimen_Mapper,
    "tumor_desc_mapper": Tumor_Desc_Mapper,
    "channel_color": Channel_Color,
}


# -----------------------------
# Modifier handling
# -----------------------------
def apply_modifier(value, modifiers, modifier_values):
    if value is None or pd.isna(value):
        return value

    if pd.isna(modifiers):
        return value

    # Parse modifiers and modifier values, handling potential mismatches
    modifier_list = [m.strip().lower() for m in str(modifiers).split(",")]
    modifier_value_list = []
    
    if not pd.isna(modifier_values):
        modifier_value_list = [v.strip() for v in str(modifier_values).split("#")]
    
    # Extend modifier_value_list with empty strings if it's shorter than modifier_list
    while len(modifier_value_list) < len(modifier_list):
        modifier_value_list.append("")

    # zip modifiers and modifier values together and apply in sequence
    for modifier, modifier_value in zip(modifier_list, modifier_value_list):
        if modifier == "suffix":
            value = f"{value}_{modifier_value}"
        elif modifier == "prefix":
            value = f"{modifier_value}_{value}"
        elif modifier == "default":
            value = modifier_value
        elif modifier == "default_bool":
            value = bool(modifier_value)
        elif modifier == "default_int":
            value = int(modifier_value)
        elif modifier == "uppercase":
            value = str(value).upper()

    return value


# -----------------------------
# Rule
# -----------------------------
class Rule:
    def __init__(self, rule_row: pd.Series):
        self.node = rule_row["target_node"]
        # Normalize mode - treat empty strings and NaN as the same
        mode_val = rule_row.get("mode")
        self.mode = (
            ""
            if (pd.isna(mode_val) or str(mode_val).strip() == "")
            else str(mode_val).strip()
        )
        self.property = rule_row["target_property"]

        self.core = rule_row.get("core") if not pd.isna(rule_row.get("core")) else None

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

        if (
            value is None or pd.isna(value) or value == "" or str(value).upper() == "NA"
        ) and pd.notna(self.required_default):
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

        self.core_rules = [r for r in rules if r.core is not None]

    def evaluate_row(self, row: pd.Series) -> Optional[Dict]:
        # Core gating - check if any core rule's input matches its expected core value
        if self.core_rules:
            core_match = False
            for r in self.core_rules:
                # Check the input data that feeds into this core rule
                for input_col in r.transform.inputs:
                    row_value = row.get(input_col)
                    core_value = r.core
                    
                    # Handle None values properly to avoid "None" string matches
                    if pd.isna(row_value) and pd.isna(core_value):
                        core_match = True
                    elif pd.isna(row_value) or pd.isna(core_value):
                        # One is None/NaN, the other isn't - no match
                        continue
                    elif str(row_value) == str(core_value):
                        core_match = True
                    
                    if core_match:
                        break
                if core_match:
                    break

            if not core_match:
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
        # Validate all rules upfront before processing
        self._validate_rules(rules_df)
        self.mode_groups = self._build_mode_groups(rules_df)

    @staticmethod
    def _validate_rules(rules_df: pd.DataFrame):
        """Validate all rules before processing to catch errors early."""
        invalid_functions = []
        missing_functions = []
        
        for idx, row in rules_df.iterrows():
            fn = row.get("function")
            
            # Check for missing functions
            if pd.isna(fn):
                missing_functions.append(f"Row {idx}: {row.get('target_node', 'unknown')}.{row.get('target_property', 'unknown')}")
                continue
            
            # Check for unsupported functions
            function_name = str(fn).strip().lower()
            if function_name not in TRANSFORM_REGISTRY:
                invalid_functions.append(f"Row {idx}: '{function_name}' for {row.get('target_node', 'unknown')}.{row.get('target_property', 'unknown')}")
        
        # Report all validation errors at once
        if missing_functions or invalid_functions:
            error_msg = "Rule validation failed:\n"
            if missing_functions:
                error_msg += f"Missing functions in:\n  " + "\n  ".join(missing_functions) + "\n"
            if invalid_functions:
                error_msg += f"Unsupported functions in:\n  " + "\n  ".join(invalid_functions) + "\n"
                error_msg += f"Available functions: {list(TRANSFORM_REGISTRY.keys())}"
            raise ValueError(error_msg)

    @staticmethod
    def _build_mode_groups(rules_df):
        groups = {}

        for _, row in rules_df.iterrows():
            rule = Rule(row)
            key = (rule.node, rule.mode)
            groups.setdefault(key, []).append(rule)

        return {key: ModeGroup(key[0], key[1], rules) for key, rules in groups.items()}

    def transform(self, target_node: str, input_df: pd.DataFrame) -> pd.DataFrame:
        node_rows: List[Dict] = []

        for _, row in input_df.iterrows():
            for (node, _mode), group in self.mode_groups.items():
                if node == target_node:
                    result = group.evaluate_row(row)
                    if result is not None:
                        node_rows.append(result)

        return pd.DataFrame(node_rows) if node_rows else pd.DataFrame()
