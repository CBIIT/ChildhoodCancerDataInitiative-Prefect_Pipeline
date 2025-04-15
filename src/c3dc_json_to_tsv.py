# Reads harmonized JSON files and produces TSV files

import csv
import hashlib
import inflect
import json
import re
import sys
import uuid
from bento_mdf import MDF
from pathlib import Path
from utils import get_logger

# String values that we must assume
FOREIGN_ID_SUFFIX = '_id'
ID_SUFFIX = '_id'
PARENT_NODE_ID = 'study_id'
PARENT_NODE_NAME = 'study'
PARENT_NODE_NAME_PLURAL = 'studies'
PRIMARY_KEY_NAME = 'id'

def main():
    # Set up logging
    logger = get_logger(__name__, 'info')

    args = sys.argv[1:]

    # Check CLI args
    if len(args) < 3:
        logger.error('Insufficient arguments. Provide 3 CLI arguments: data directory, model MDF path, and props MDF path.')
        return

    # Read CLI args
    data_dir = Path(args[0]).resolve()
    model_mdf_path = Path(args[1]).resolve()
    props_mdf_path = Path(args[2]).resolve()

    # Node names we expect from the JSON files
    model = get_datamodel(model_mdf_path, props_mdf_path)
    dir_paths = get_data_subdirectories(data_dir)

    logger.info(f'Found {len(dir_paths)} subdirectories\n')

    process_json_files(dir_paths, model, logger)

def foreign_id_name(foreign_node_name):
    """ Get the foreign ID name of a foreign node

    Args:
        foreign_node_name (str): The name of the foreign node

    Returns:
        str: The foreign ID name
    """

    return f'{foreign_node_name}.{foreign_node_name}{FOREIGN_ID_SUFFIX}'

def foreign_key_name(foreign_node_name):
    """ Get the foreign key name of a foreign node

    Args:
        foreign_node_name (str): The name of the foreign node

    Returns:
        str: The foreign key name
    """

    return f'{foreign_node_name}.{PRIMARY_KEY_NAME}'

def get_data_subdirectories(data_dir):
    """ Get subdirectories to read data from

    Args:
        data_dir (Path): The path to the data directory

    Returns:
        list: List of data subdirectories
    """

    dir_paths = [path for path in data_dir.iterdir() if path.is_dir()]

    return dir_paths

def get_datamodel(model_mdf_path, props_mdf_path):
    """ Get the datamodel

    Args:
        model_mdf_path (Path): The path to the datamodel's model MDF file
        props_mdf_path (Path): The path to the datamodel's props MDF file

    Returns:
        Model: Object representation of the datamodel
    """

    mdf_from_file = MDF(model_mdf_path, props_mdf_path, handle='C3DC')

    return mdf_from_file.model

def get_file_paths(dir_path):
    """ Get paths of JSON files in a directory

    Args:
        dir_path (Path): The directory in which to look for files

    Returns:
        list: A list of JSON file paths
    """

    file_paths = []

    # Look in this subdirectory for individual JSON files to group into a single study
    for file_path in dir_path.iterdir():
        if not file_path.is_file():
            continue

        if file_path.suffix.lower() != '.json':
            continue

        # Add JSON file to list
        file_paths.append(file_path)

    return file_paths

def get_foreign_ids(model, node_name):
    """ Gets ID property names of foreign nodes
    Args:
        model (Model): The datamodel
        node_name (str): The name of the node

    Returns:
        list: List of ID property names of foreign nodes
    """

    foreign_ids = []

    for edge in model.edges.values():
        destination_node_name = edge.dst.handle
        source_node_name = edge.src.handle

        if source_node_name != node_name:
            continue

        foreign_ids.append(f'{foreign_id_name(destination_node_name)}')

    return foreign_ids

def get_foreign_keys(model, node_name):
    """ Gets foreign key property names of foreign nodes
    Args:
        model (Model): The datamodel
        node_name (str): The name of the node

    Returns:
        list: List of foreign key property names of foreign nodes
    """

    foreign_keys = []

    for edge in model.edges.values():
        destination_node_name = edge.dst.handle
        source_node_name = edge.src.handle

        if source_node_name != node_name:
            continue

        foreign_keys.append(f'{foreign_key_name(destination_node_name)}')

    return foreign_keys

def get_parent_node_names(model, node_name):
    """ Gets names of parent nodes
    Args:
        model (Model): The datamodel
        node_name (str): The name of the node

    Returns:
        list: List of names of parent nodes
    """

    foreign_keys = []

    for edge in model.edges.values():
        destination_node_name = edge.dst.handle
        source_node_name = edge.src.handle

        if source_node_name != node_name:
            continue

        foreign_keys.append(destination_node_name)

    return foreign_keys

def get_study_id(data):
    """ Get the value of study_id

    Args:
        data (dict): The JSON data for the whole study

    Raises:
        Exception: It's an error to not have a study
        Exception: It's an error to have multiple unique studies

    Returns:
        str: The value of study_id
    """
    study_id = None
    study_records = data.get(PARENT_NODE_NAME_PLURAL)

    if not study_records:
        raise Exception(f'No {PARENT_NODE_NAME} records to parse!')

    for study_data in study_records:
        # There should only be one unique Study record
        if study_id and study_data.get(PARENT_NODE_ID) != study_id:
            raise Exception(f'More than one unique {PARENT_NODE_NAME} record found: {study_id}')

        study_id = study_data.get(PARENT_NODE_ID)

    return study_id

def id_name(node_name):
    """ Get the ID name of a node

    Args:
        node_name (str): The name of the node

    Returns:
        str: The ID name
    """

    return f'{node_name}{ID_SUFFIX}'

def make_uuid(node_type, study_id, row, foreign_ids):
    """ Makes a UUID for a record

    Args:
        node_type (str): The name of the node type
        study_id (str): The study_id value of the record's study
        row (dict): The record as a map of property names to property values
        foreign_ids (list): The record's properties that are foreign IDs

    Returns:
        str: The record's UUID
    """

    row_str = ''
    foreign_str = ''.join([
        str(row[foreign_id]) for foreign_id in foreign_ids
    ]) if foreign_ids else ''

    if node_type == 'participant':
        row_str = row['participant_id']
    elif node_type == 'reference_file':
        row_str = row['reference_file_url']
    else:
        id_pattern = fr'\b\w+{FOREIGN_ID_SUFFIX}\b'
        identifying_props = [
            prop for prop in row if prop != PRIMARY_KEY_NAME and not re.fullmatch(id_pattern, prop)
        ]
        row_str = ''.join([
            str(row[prop]) for prop in identifying_props
        ]) if identifying_props else ''

    uuid_name = ''.join([
        node_type,
        study_id,
        row_str,
        foreign_str
    ])

    return str(uuid.uuid5(uuid.NAMESPACE_URL, uuid_name))

def pluralize(word):
    """ Pluralizes a word

    Args:
        word (string): The word to pluralize

    Returns:
        plural: The pluralized word
    """

    p = inflect.engine()

    return p.plural(word)

def process_json_files(dir_paths, model, logger):
    """ Writes JSON data into TSV files

    Args:
        dir_paths (list): Paths to JSON files
        model (Model): The datamodel
    """

    uuids = set() # Hashes of previously encountered primary keys

    node_names_to_plural = {
        name: pluralize(name) for name in model.nodes.keys()
    }
    node_names_plural = list(node_names_to_plural.values())

    # Look at all the files in the data directory, grouping files within a subdirectory into a single study
    for dir_path in dir_paths:
        file_paths = get_file_paths(dir_path)
        all_json_data = dict.fromkeys(node_names_plural, [])
        all_records = {} # Stores records read from JSON

        logger.info(f'Found {len(file_paths)} JSON file(s) in subdirectory {dir_path}\n')

        # Skip if no JSON files in subdirectory
        if not file_paths:
            continue

        for file_path in file_paths:
            logger.info(f'Reading data from {file_path}...\n')

            json_file = open(file_path, encoding='utf8')
            json_data = json.load(json_file)

            for node_name_plural in node_names_plural:
                if node_name_plural not in json_data:
                    continue

                all_json_data[node_name_plural] = all_json_data.get(node_name_plural) + json_data.get(node_name_plural)

            json_file.close()
            logger.info(f'Finished reading data from {file_path}...\n')

        # Parse study_id first, because UUIDs need it
        logger.info(f'Parsing {PARENT_NODE_ID} from JSON...\n')
        study_id = get_study_id(all_json_data)

        # Store data from JSON in a dict
        for (node_name, node_name_plural) in node_names_to_plural.items():
            if node_name not in all_records:
                all_records[node_name] = {}

            logger.info(f'Parsing {node_name} records from JSON...\n')

            records = all_json_data.get(node_name_plural)

            if not records:
                logger.warning(f'No {node_name} records! Skipping {node_name}...\n')
                continue

            foreign_ids = get_foreign_ids(model, node_name)
            node = model.nodes.get(node_name)
            props = node.props

            # Assign UUIDs
            for record in records:
                record_id = record.get(f'{id_name(node_name)}')
                row = read_record(record, props, foreign_ids)
                uuid = make_uuid(node_name, study_id, row, foreign_ids)

                # Skip record if duplicate
                if uuid in uuids:
                    logger.warning(f'Duplicate {node_name} record {record_id} found (UUID {uuid})')
                    continue

                uuids.add(uuid)
                row[PRIMARY_KEY_NAME] = uuid
                all_records[node_name][record_id] = row

            logger.info(f'Finished parsing {node_name} records\n')

        # Assign foreign keys to records
        for (node_name, _) in node_names_to_plural.items():
            logger.info(f'Assigning foreign keys to {node_name} records...\n')

            if not all_records[node_name]:
                logger.warning(f'No {node_name} records! Skipping {node_name}...\n')
                continue

            parent_node_names = get_parent_node_names(model, node_name)

            for record_id in all_records[node_name]:
                # Assign foreign keys to record
                for parent_node_name in parent_node_names:
                    foreign_key = None
                    parent_id = all_records[node_name][record_id].get(foreign_id_name(parent_node_name))

                    if parent_id:
                        parent_node = all_records[parent_node_name][parent_id]
                        foreign_key = parent_node.get(PRIMARY_KEY_NAME)

                    all_records[node_name][record_id][foreign_key_name(parent_node_name)] = foreign_key

                    # Foreign ID no longer needed
                    del all_records[node_name][record_id][foreign_id_name(parent_node_name)]

            logger.info(f'Finished assigning foreign keys to {node_name} records\n')

        # Write a TSV file for each node type
        for (node_name, node_name_plural) in node_names_to_plural.items():
            logger.info(f'Writing {node_name} records to TSV...')

            if not all_records[node_name]:
                logger.warning(f'No {node_name} records! Skipping {node_name}...\n')
                continue

            foreign_keys = get_foreign_keys(model, node_name)
            node = model.nodes.get(node_name)
            props = node.props
            tsv_path = Path.cwd() / 'data' / f'{study_id} {node_name_plural}.tsv'

            # Write TSV
            with open(tsv_path, 'w', encoding='utf-8', newline='') as tsv_file:
                # Write the column headers
                tsv_headers = ['type'] + list(props.keys()) + foreign_keys
                tsv_writer = csv.writer(tsv_file, delimiter='\t', dialect='unix')
                tsv_writer.writerow(tsv_headers)

                # Write each record to a TSV row
                for record in all_records[node_name].values():
                    tsv_writer.writerow(to_tsv_row(record, node_name, props, foreign_keys))

                tsv_file.close()

            logger.info(f'Finished writing {node_name} records to TSV\n')
            logger.info(f'Finished parsing {node_name} records\n')

def read_record(record, props, foreign_ids):
    """ Reads properties from a record

    Args:
        record (dict): JSON of record
        props (dict): Properties to read from the record
        foreign_ids (list): Foreign ..._id properties to read from the record

    Returns:
        dict: Map of property names to property values
    """

    row = {}

    # Read regular properties
    for (prop_name, prop) in props.items():
        # Primary keys aren't in the JSON - we're the ones who make primary keys
        if prop_name == PRIMARY_KEY_NAME:
            continue

        data = record.get(prop_name)

        if data and prop.value_domain == 'list':
            data = ';'.join(data)

        row[prop_name] = data

    # Read foreign properties
    for foreign_id in foreign_ids:
        row[foreign_id] = record.get(foreign_id)

    return row

def to_tsv_row(record, node_name, props, foreign_keys):
    """ Converts a record to a TSV row

    Args:
        record (dict): The record
        node_name (str): The name of the node
        props (dict): Properties to read from the record
        foreign_keys (list): Foreign key properties to read from the record

    Returns:
        list: List of property values
    """

    native_vals = [
        record[prop_name] for prop_name in props.keys()
    ]
    foreign_vals = [
        record[foreign_key] for foreign_key in foreign_keys
    ]

    return [node_name] + native_vals + foreign_vals

if __name__ == '__main__':
    main()
