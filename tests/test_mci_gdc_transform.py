import os
import sys
import pandas as pd
import numpy as np
import pytest
import tempfile
import json
import logging
from unittest.mock import MagicMock, patch, mock_open
from io import StringIO

parent_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir)

# Import functions from mci_gdc_transform.py
from workflows.mci_gdc_transform import (
    survival_status_parser,
    diagnosis_parser,
    fastq_parser,
    extract_metadata_to_tsv,
    sample_parser,
    methylation_parser,
    convert_tsv_json,
    validate_graph,
    validate_pvs,
)


# Test fixtures
@pytest.fixture
def sample_participant_df():
    """Sample participant DataFrame for testing."""
    return pd.DataFrame({
        'participant_id': ['P001', 'P002', 'P003'],
        'age_at_enrollment': [5, 10, 15],
        'sex': ['Male', 'Female', 'Male']
    })


@pytest.fixture
def sample_survival_df():
    """Sample survival DataFrame for testing."""
    return pd.DataFrame({
        'participant.participant_id': ['P001', 'P001', 'P002', 'P003'],
        'age_at_last_known_survival_status': [365, 730, 500, 200],
        'last_known_survival_status': ['Alive', 'Dead', 'Alive', 'Alive']
    })


@pytest.fixture
def sample_diagnosis_df():
    """Sample diagnosis DataFrame for testing."""
    return pd.DataFrame({
        'participant.participant_id': ['P001', 'P002', 'P003', 'P004'],
        'diagnosis_id': ['D001', 'D002', 'D003', 'D004'],
        'diagnosis_classification_system': ['ICD-O-3.2', 'ICD-O-3.2', 'ICD-10', 'ICD-O-3.2'],
        'diagnosis_category': ['Cancer', 'Cancer', 'Other', 'Cancer'],
        'diagnosis': ['Leukemia', 'Lymphoma', 'Diabetes', 'Sarcoma'],
        'anatomic_site': ['Blood', 'Lymph', 'Pancreas', 'Bone'],
        'age_at_diagnosis': [120, 240, 180, 300]
    })


@pytest.fixture
def sample_sequencing_file_df():
    """Sample sequencing file DataFrame for testing."""
    return pd.DataFrame({
        'file_name': ['file1.fastq', 'file2.bam', 'file3.fastq', 'file4.fastq'],
        'file_type': ['fastq', 'bam', 'fastq', 'fastq'],
        'library_strategy': ['WXS', 'WGS', 'RNA-Seq', 'ChIP-Seq']
    })


@pytest.fixture
def sample_sample_df():
    """Sample sample DataFrame for testing."""
    return pd.DataFrame({
        'sample_id': ['S001', 'S002', 'S003'],
        'sample_type': ['Primary', 'Metastatic', 'Normal'],
        'participant_id': ['P001', 'P002', 'P003']
    })


@pytest.fixture
def sample_preservation_platform_df():
    """Sample preservation method and platform DataFrame for testing."""
    return pd.DataFrame({
        'sample_id': ['S001', 'S002', 'S003'],
        'preservation_method': ['FFPE', 'Fresh Frozen', 'FFPE'],
        'platform': ['IlluminaHumanMethylationEPIC', 'IlluminaHumanMethylationEPICv2', 'WES']
    })


@pytest.fixture
def sample_methylation_array_file_df():
    """Sample methylation array file DataFrame for testing."""
    return pd.DataFrame({
        'file_name': ['meth1.idat', 'meth2.txt', 'meth3.idat'],
        'file_type': ['idat', 'txt', 'idat'],
        'sample.sample_id': ['S001', 'S002', 'S003']
    })


@pytest.fixture
def temp_preservation_file():
    """Create a temporary preservation method platform file."""
    df = pd.DataFrame({
        'sample_id': ['S001', 'S002', 'S003'],
        'preservation_method': ['FFPE', 'Fresh Frozen', None],
        'platform': ['IlluminaHumanMethylationEPIC', 'IlluminaHumanMethylationEPICv2', 'WES']
    })
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv') as f:
        df.to_csv(f, sep='\t', index=False)
        return f.name


@pytest.fixture
def sample_json_files_dir():
    """Create a temporary directory with sample JSON files."""
    temp_dir = tempfile.mkdtemp()
    
    # Create rawdata JSON file
    rawdata_content = {
        "meta_data": {
            "array_type": "IlluminaHumanMethylationEPIC",
            "material_type": "FFPE",
            "ID": "RAW_S001_data"
        }
    }
    
    # Create other JSON file
    other_content = {
        "metadata": {
            "sample_name": "SAMPLE-S002-processed",
            "ffpe": True,
            "data_type": "RNA-Seq"
        }
    }
    
    with open(os.path.join(temp_dir, 'rawdata_sample.json'), 'w') as f:
        json.dump(rawdata_content, f)
    
    with open(os.path.join(temp_dir, 'other_sample.json'), 'w') as f:
        json.dump(other_content, f)
    
    return temp_dir


class TestSurvivalStatusParser:
    """Test cases for survival_status_parser function."""
    
    def test_survival_status_parser_basic(self, sample_participant_df, sample_survival_df):
        """Test basic functionality of survival status parser."""
        result = survival_status_parser(sample_participant_df, sample_survival_df)
        
        # Check that the result has the expected columns
        assert 'last_known_survival_status' in result.columns
        assert 'participant.participant_id' in result.columns
        
        # Check that most recent survival status is selected (P001 should have 'Dead')
        p001_status = result[result['participant_id'] == 'P001']['last_known_survival_status'].iloc[0]
        assert p001_status == 'Dead'
    
    def test_survival_status_parser_no_survival_data(self, sample_participant_df):
        """Test survival status parser with empty survival DataFrame."""
        empty_survival = pd.DataFrame(columns=['participant.participant_id', 'age_at_last_known_survival_status', 'last_known_survival_status'])
        
        result = survival_status_parser(sample_participant_df, empty_survival)
        
        # Should return original participant df with NaN survival status
        assert len(result) == len(sample_participant_df)
        assert 'last_known_survival_status' in result.columns


class TestDiagnosisParser:
    """Test cases for diagnosis_parser function."""
    
    def test_diagnosis_parser_basic(self, sample_participant_df, sample_diagnosis_df):
        """Test basic functionality of diagnosis parser."""
        participant_result, diagnosis_result = diagnosis_parser(sample_participant_df, sample_diagnosis_df)
        
        # Check that ICD-O-3.2 diagnoses are filtered
        assert len(diagnosis_result) == 3  # Should exclude ICD-10 diagnosis
        
        # Check that participant df is merged with diagnosis info
        assert 'diagnosis_id' in participant_result.columns
        assert 'diagnosis_category' in participant_result.columns
    
    def test_diagnosis_parser_no_icd_diagnoses(self, sample_participant_df):
        """Test diagnosis parser with no ICD-O-3.2 diagnoses."""
        no_icd_diagnosis = pd.DataFrame({
            'participant.participant_id': ['P001'],
            'diagnosis_classification_system': ['ICD-10'],
            'diagnosis_id': ['D001'],
            'diagnosis_category': ['Other'],
            'diagnosis': ['Diabetes'],
            'anatomic_site': ['Pancreas'],
            'age_at_diagnosis': [180]
        })
        
        participant_result, diagnosis_result = diagnosis_parser(sample_participant_df, no_icd_diagnosis)
        
        # Should return empty diagnosis result
        assert len(diagnosis_result) == 0


class TestFastqParser:
    """Test cases for fastq_parser function."""
    
    def test_fastq_parser_basic(self, sample_sequencing_file_df):
        """Test basic functionality of FASTQ parser."""
        result = fastq_parser(sample_sequencing_file_df)
        
        # Should only include FASTQ files with WXS or RNA-Seq
        assert len(result) == 2
        assert all(result['file_type'] == 'fastq')
        assert all(result['library_strategy'].isin(['WXS', 'RNA-Seq']))
    
    def test_fastq_parser_no_matching_files(self):
        """Test FASTQ parser with no matching files."""
        df = pd.DataFrame({
            'file_name': ['file1.bam'],
            'file_type': ['bam'],
            'library_strategy': ['WGS']
        })
        
        result = fastq_parser(df)
        assert len(result) == 0


class TestExtractMetadataToTsv:
    """Test cases for extract_metadata_to_tsv function."""
    
    def test_extract_metadata_to_tsv_basic(self, sample_json_files_dir):
        """Test basic functionality of metadata extraction."""
        output_file = os.path.join(sample_json_files_dir, 'output.tsv')
        
        extract_metadata_to_tsv(sample_json_files_dir, output_file)
        
        # Check that output file was created
        assert os.path.exists(output_file)
        
        # Check content
        result_df = pd.read_csv(output_file, sep='\t')
        assert len(result_df) == 2
        assert 'sample_id' in result_df.columns
        assert 'platform' in result_df.columns
        assert 'preservation_method' in result_df.columns
    
    def test_extract_metadata_to_tsv_nonexistent_directory(self):
        """Test extract metadata with non-existent directory."""
        with tempfile.NamedTemporaryFile(suffix='.tsv') as temp_output:
            # Should handle gracefully and not crash
            extract_metadata_to_tsv('/nonexistent/directory', temp_output.name)


class TestSampleParser:
    """Test cases for sample_parser function."""
    
    def test_sample_parser_basic(self, sample_sample_df, temp_preservation_file):
        """Test basic functionality of sample parser."""
        result = sample_parser(sample_sample_df, temp_preservation_file)
        
        # Check that preservation method is added
        assert 'preservation_method' in result.columns
        
        # Check that missing values are filled with "Not Reported"
        assert result['preservation_method'].notna().all()
        assert "Not Reported" in result['preservation_method'].values
        
        # Cleanup
        os.unlink(temp_preservation_file)
    
    def test_sample_parser_all_missing_preservation(self, sample_sample_df):
        """Test sample parser with all missing preservation methods."""
        # Create temp file with all null preservation methods
        df = pd.DataFrame({
            'sample_id': ['S001', 'S002', 'S003'],
            'preservation_method': [None, None, None]
        })
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv') as f:
            df.to_csv(f, sep='\t', index=False)
            temp_file = f.name
        
        result = sample_parser(sample_sample_df, temp_file)
        
        # All should be "Not Reported"
        assert all(result['preservation_method'] == 'Not Reported')
        
        # Cleanup
        os.unlink(temp_file)


class TestMethylationParser:
    """Test cases for methylation_parser function."""
    
    def test_methylation_parser_basic(self, sample_methylation_array_file_df, temp_preservation_file):
        """Test basic functionality of methylation parser."""
        result = methylation_parser(sample_methylation_array_file_df, temp_preservation_file)
        
        # Should only include IDAT files
        assert all(result['file_type'] == 'idat')
        assert len(result) == 2  # Only 2 IDAT files in sample data
        
        # Check platform mapping
        assert 'platform' in result.columns
        
        # Cleanup
        os.unlink(temp_preservation_file)
    
    def test_methylation_parser_platform_mapping(self, sample_methylation_array_file_df, temp_preservation_file):
        """Test platform value mapping in methylation parser."""
        result = methylation_parser(sample_methylation_array_file_df, temp_preservation_file)
        
        # Check that platform mapping is applied
        expected_mappings = {
            'IlluminaHumanMethylationEPIC': 'Illumina Methylation Epic',
            'IlluminaHumanMethylationEPICv2': 'Illumina Methylation Epic v2'
        }
        
        for original, mapped in expected_mappings.items():
            if original in result['platform'].values:
                assert mapped in result['platform'].values
        
        # Cleanup
        os.unlink(temp_preservation_file)


class TestConvertTsvJson:
    """Test cases for convert_tsv_json function."""
    
    def test_convert_tsv_json_basic(self):
        """Test basic functionality of TSV to JSON conversion."""
        # Create sample DataFrame
        df = pd.DataFrame({
            'type': ['sample', 'sample'],
            'submitter_id': ['C001', 'C002'],
            'cases.submitter_id': ['C001', 'C002']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            output_file = f.name
        
        convert_tsv_json(df, output_file)
        
        # Check that JSON file was created
        assert os.path.exists(output_file)
        
        # Check content
        with open(output_file, 'r') as f:
            json_data = json.load(f)
        
        assert len(json_data) == 2
        assert 'cases' in json_data[0]  # Should be restructured
        
        # Cleanup
        os.unlink(output_file)
    
    def test_convert_tsv_json_unknown_node_type(self):
        """Test JSON conversion with unknown node type."""
        df = pd.DataFrame({
            'submitter_id': ['X001', 'X002'],
            'some_field': ['value1', 'value2']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            output_file = f.name
        
        convert_tsv_json(df, output_file)
        
        # Should handle gracefully
        assert os.path.exists(output_file)
        
        # Cleanup
        os.unlink(output_file)


class TestValidateGraph:
    """Test cases for validate_graph function."""
    
    def test_validate_graph_valid_data(self):
        """Test graph validation with valid data."""
        output_dfs = {
            'case': pd.DataFrame({'submitter_id': ['C001', 'C002']}),
            'sample': pd.DataFrame({
                'submitter_id': ['S001', 'S002'],
                'cases.submitter_id': ['C001', 'C002']
            })
        }
        
        logger = MagicMock()
        
        # Should not log any errors
        validate_graph(output_dfs, logger)
        logger.error.assert_not_called()
    
    def test_validate_graph_missing_parent_ids(self):
        """Test graph validation with missing parent IDs."""
        output_dfs = {
            'case': pd.DataFrame({'submitter_id': ['C001']}),
            'sample': pd.DataFrame({
                'submitter_id': ['S001', 'S002'],
                'cases.submitter_id': ['C001', 'C003']  # C003 doesn't exist in case
            })
        }
        
        logger = MagicMock()
        
        validate_graph(output_dfs, logger)
        logger.error.assert_called()
    
    def test_validate_graph_missing_id_column(self):
        """Test graph validation with missing ID column."""
        output_dfs = {
            'case': pd.DataFrame({'submitter_id': ['C001', 'C002']}),
            'sample': pd.DataFrame({
                'submitter_id': ['S001', 'S002']
                # Missing 'cases.submitter_id' column
            })
        }
        
        logger = MagicMock()
        
        validate_graph(output_dfs, logger)
        logger.error.assert_called()


class TestValidatePvs:
    """Test cases for validate_pvs function."""
    
    @patch('requests.get')
    def test_validate_pvs_valid_data(self, mock_get):
        """Test PV validation with valid data."""
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'properties': {
                'gender': {'enum': ['male', 'female', 'unknown']},
                'vital_status': {'enum': ['alive', 'dead', 'unknown']}
            }
        }
        mock_get.return_value = mock_response
        
        output_dfs = {
            'case': pd.DataFrame({
                'gender': ['male', 'female'],
                'vital_status': ['alive', 'dead']
            })
        }
        
        logger = MagicMock()
        
        validate_pvs(output_dfs, logger)
        logger.error.assert_not_called()
    
    @patch('requests.get')
    def test_validate_pvs_invalid_data(self, mock_get):
        """Test PV validation with invalid data."""
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'properties': {
                'gender': {'enum': ['male', 'female', 'unknown']},
                'vital_status': {'enum': ['alive', 'dead', 'unknown']}
            }
        }
        mock_get.return_value = mock_response
        
        output_dfs = {
            'case': pd.DataFrame({
                'gender': ['male', 'invalid_gender'],  # Invalid value
                'vital_status': ['alive', 'dead']
            })
        }
        
        logger = MagicMock()
        
        validate_pvs(output_dfs, logger)
        logger.error.assert_called()



if __name__ == "__main__":
    pytest.main([__file__])