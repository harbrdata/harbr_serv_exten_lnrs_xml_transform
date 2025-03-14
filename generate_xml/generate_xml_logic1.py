import os
from collections import defaultdict
from multiprocessing import Pool
from multiprocessing import cpu_count

import polars as pl
from lxml import etree
from tqdm import tqdm

# Mapping of parquet files to class names
parquet_map = {
    'entity': 'Entity',
    'entityaddress': 'Address',
    'entityalias': 'Alias',
    'entitycountryassociation': 'CountryAssociation',
    'entitydob': 'DOB',
    'entityidentifications': 'Identification',
    'entityposition': 'Position',
    'entityrelationships': 'Relationship',
    'entityremark': 'Remark',
    'entitysourceitem': 'SourceItem',
    'entityadversemedia': 'EntityAdverseMedia',
    'entityenforcement': 'EntityEnforcement',
    'entityenforcementsubcategory': 'EntityEnforcementSubcategory',
    'entitypep': 'EntityPEP',
    'entitypepsubcategory': 'EntityPEPSubcategory',
    'entitysanction': 'EntitySanction',
    'consolidatedsanction': 'EntitySanctionConsolidatedSanction',
    'entitysoe': 'EntitySOE',
    'entitysoedomain': 'EntitySOEEntitySOEDomain',
    'entitysoesubcategory': 'EntitySOESubcategory',
    'entityidentification': 'Identification',
    'entitydeletes': 'EntityDeletes',
}

# Mapping from parent tables to child tables
child_tables_map = {
    'entityenforcement': 'entityenforcementsubcategory',
    'entitypep': 'entitypepsubcategory',
    'entitysanction': 'consolidatedsanction',
    'entitysoe': ['entitysoedomain', 'entitysoesubcategory'],
}

# Mapping for GUID columns
guid_col_map = {
    'entityenforcement': 'entityenforcementguid',
    'entitypep': 'entitypepguid',
    'entitysanction': 'entitysanctionguid',
    'entitysoe': 'entitysoeguid',
}

additional_segments_tables_simple = {
    'associatedentity': {
        'guid_col': 'associatedentityguid',
        'type': 'Associated Entity'
    },
    'fatcatreginst': {
        'guid_col': 'fatcatreginstguid',
        'type': 'FATCA Reg Inst'
    },
    'marijuanaregbus': {
        'guid_col': 'marijuanaregbusguid',
        'type': 'Marijuana Reg Bus'
    },
    'ownershiporcontrol': {
        'guid_col': 'ownershiporcontrolguid',
        'type': 'Ownership Or Control'
    },
    'swiftbicentity': {
        'guid_col': 'swiftbicentityguid',
        'type': 'SWIFT BIC Entity'
    }
}

additional_segments_tables_mutifield = {
    'ihsofacvessels': {
        "guid_col": "ihsofacvesselsguid",
        "type": "IHS OFAC Vessels"
    },
    'ihsregvessels': {
        "guid_col": "ihsregvesselsguid",
        "type": "IHS Reg Vessels"
    },
    'uaemsb': {
        "guid_col": "uaemsbguid",
        "type": "UAE MSB"
    },
    'usmsb': {
        "guid_col": "usmsbguid",
        "type": "US MSB"
    }
}


def parse_xsd(xsd_file_path):
    """
    Parses an XSD file to extract constraints and a name map.

    Produces:
    - constraints: { "/WCOData/Entities/Entity": { minOccurs, maxOccurs, name }, ... }
    - name_map: { parentPath: { lowerChildName: childNameExact } }
    """
    tree = etree.parse(xsd_file_path)
    root = tree.getroot()
    ns = {'xs': 'http://www.w3.org/2001/XMLSchema'}

    constraints = {}
    name_map = defaultdict(dict)

    def recurse_elements(node, current_path):
        for child in node:
            localname = etree.QName(child).localname
            if localname == 'element':
                ename = child.get('name')
                if ename:
                    full_path = f"{current_path}/{ename}"
                    min_occ_str = child.get('minOccurs', '0')
                    max_occ_str = child.get('maxOccurs', 'unbounded')
                    min_val = int(min_occ_str)
                    max_val = float('inf') if max_occ_str.lower() == 'unbounded' else int(max_occ_str)
                    constraints[full_path] = {'minOccurs': min_val, 'maxOccurs': max_val, 'name': ename}
                    name_map[current_path][ename.lower()] = ename
                recurse_elements(child, full_path)
            elif localname in ['complexType', 'sequence', 'all', 'choice']:
                recurse_elements(child, current_path)

    # Handle top-level elements
    for top_elem in root.findall('.//xs:element', ns):
        top_name = top_elem.get('name')
        if top_name:
            top_path = f"/{top_name}"
            min_occ_str = top_elem.get('minOccurs', '0')
            max_occ_str = top_elem.get('maxOccurs', 'unbounded')
            min_val = int(min_occ_str)
            max_val = float('inf') if max_occ_str.lower() == 'unbounded' else int(max_occ_str)
            constraints[top_path] = {'minOccurs': min_val, 'maxOccurs': max_val, 'name': top_name}
            name_map[""][top_name.lower()] = top_name
            recurse_elements(top_elem, top_path)

    return constraints, name_map


def detect_container_map(constraints):
    """
    Detects container relationships (e.g., "EntityAddresses" containing repeated "EntityAddress").

    Returns a dict: { lowerChildName: (containerName, childNameLower) }
    """
    children_by_parent = defaultdict(list)
    for full_path, info in constraints.items():
        split_parts = full_path.split("/")
        if len(split_parts) > 1:
            parent_path = "/".join(split_parts[:-1])
            children_by_parent[parent_path].append((full_path, info))

    container_map = {}
    for parent_path, child_list in children_by_parent.items():
        # If exactly one child with maxOccurs > 1, interpret parent as a container
        if len(child_list) == 1:
            child_full_path, child_info = child_list[0]
            if child_info['maxOccurs'] == float('inf') or child_info['maxOccurs'] > 1:
                parent_name = constraints[parent_path]['name']
                child_name = child_info['name']
                container_map[child_name.lower()] = (parent_name, child_name.lower())

    return container_map


def get_required_parquet_tables(xsd_element_names, parquet_map):
    """
    From your XSD's discovered element names, figure out which tables from `parquet_map` we actually need to load.
    """
    lower_xsd = set(x.lower() for x in xsd_element_names)
    required = []
    for tname in parquet_map.keys():
        if tname in lower_xsd:
            required.append(tname)
    required.append('custom_feed_entity_match_type_lookup')
    if 'AdditionalSegments' in xsd_element_names:
        required.extend(additional_segments_tables_simple.keys())
        required.extend(additional_segments_tables_mutifield)
    return required


def load_parquet_data_polars_lazy(directory: str, table_names: list[str]):
    """
    Returns a dict of {table_name: LazyFrame}, scanning parquet files in each subfolder.
    """
    dataframes = {}
    for name in table_names:
        folder = os.path.join(directory, name)
        if os.path.isdir(folder):
            parquet_files = [
                os.path.join(folder, f)
                for f in os.listdir(folder)
            ]
            if parquet_files:
                # Create a single LazyFrame by concatenating multiple scans
                lazy_scans = [pl.scan_parquet(x, low_memory=True) for x in parquet_files]
                dataframes[name] = pl.concat(lazy_scans, how="vertical")
    return dataframes


def merge_tables(df_map):
    child_tables = []
    for parent_table, child_table_or_list in child_tables_map.items():
        if parent_table not in df_map:
            continue

        child_tables = (
            child_table_or_list
            if isinstance(child_table_or_list, list)
            else [child_table_or_list]
        )

        # Retrieve the name of the GUID column for the parent
        parent_guid_col = guid_col_map[parent_table]

        # Retrieve the parent DataFrame
        parent_df = df_map[parent_table]

        for child_table in child_tables:
            if child_table not in df_map:
                continue
            # Get the child's DataFrame
            child_df = df_map[child_table]

            grouped = (
                child_df
                .group_by(parent_guid_col)
                .agg([pl.struct(pl.all()).alias(child_table)])
            )

            # Join to entity
            x = (
                parent_df
                .join(grouped, on=parent_guid_col, how="left")
            )

            df_map[parent_table] = x
        # After merging all child tables, place it back in df_map
        df_map[parent_table] = parent_df

    entity_df = df_map["entity"]

    for table_name, child_df in df_map.items():
        if table_name == "entity" or table_name in child_tables:
            continue
        if "entityguid" in child_df.collect_schema().names():
            # Group by entityguid
            grouped = (
                child_df
                .group_by("entityguid")
                .agg([pl.struct(pl.all()).alias(table_name)])
            )

            # Join to entity
            entity_df = (
                entity_df
                .join(grouped, on="entityguid", how="left")
            )

    if "custom_feed_entity_match_type_lookup" in df_map:
        custom_feed_entity_match_type_lookup = df_map["custom_feed_entity_match_type_lookup"]
        entity_df = (
            entity_df
            .join(custom_feed_entity_match_type_lookup, on="entityguid", how="left")
        )
    return entity_df


def build_wco_data_polars_lazy(df: pl.DataFrame, entitydeletes_df: pl.DataFrame):
    """
    Builds a wco_data structure using Polars lazy queries:
      wco_data = {
         'entities': [...python dicts of entity data...],
         'relationships': [...python dicts of relationship data...]
      }
    """
    wco_data = {}
    entity_df = df.filter(pl.col("entity_match_type") == "matched_entity")
    entity_list = entity_df.collect().to_dicts()
    relations_df = df.filter(pl.col("entity_match_type") == "related_entity")
    relations_list = relations_df.collect().to_dicts()
    entitydeletes_list = []
    if entitydeletes_df is not None:
        entitydeletes_list = entitydeletes_df.collect().to_dicts()

    entity_list = [i for i in entity_list if any(i.get(j) for j in additional_segments_tables_mutifield) and any(i.get(j) for j in additional_segments_tables_simple)]
    wco_data["entities"] = entity_list[:1]
    wco_data["relationships"] = relations_list[:1]
    wco_data['entitydeletes'] = entitydeletes_list[:1]
    return wco_data


def create_single_segment_for_sigle_fields(parent_xml, data, table_name):
    data = data[0]
    segment = etree.SubElement(parent_xml, "Segment")
    segment.set("Type", additional_segments_tables_simple.get(table_name).get("type"))
    record = etree.SubElement(segment, "Record")
    record.set("GUID", data[additional_segments_tables_simple.get(table_name).get("guid_col")])
    last_updated = etree.SubElement(record, "LastUpdated")
    last_updated.text = data['lastupdated']

    field = etree.SubElement(record, "Field")
    derived_name = etree.SubElement(field, "DerivedName")
    derived_name.text = 'Source Name'
    derived_value = etree.SubElement(field, "DerivedValue")
    derived_value.text = data['source_name']

    return segment


def create_single_segment_for_mutifield(parent_xml, data, table_name):
    data_first = data[0]
    segment = etree.SubElement(parent_xml, "Segment")
    segment.set("Type", additional_segments_tables_mutifield.get(table_name).get("type"))
    record = etree.SubElement(segment, "Record")
    record.set("GUID", data_first[additional_segments_tables_mutifield.get(table_name).get("guid_col")])
    last_updated = etree.SubElement(record, "LastUpdated")
    last_updated.text = data_first['lastupdated']

    for i in data:
        field = etree.SubElement(record, "Field")
        derived_name = etree.SubElement(field, "DerivedName")
        derived_name.text = table_name.replace('_', ' ').title()
        derived_value = etree.SubElement(field, "DerivedValue")
        derived_value.text = i['source_name']

    return segment


def populate_children(parent_xml, parent_path, data_obj, constraints, name_map, container_map):
    """
    Recursively populates children of parent_xml using data_obj.
    The logic follows the original approach, but factored out as
    a top-level function so that it's pickleable for multiprocessing.
    """
    known_map = name_map.get(parent_path, {})

    for child_lower, real_tag in known_map.items():
        full_path = parent_path + "/" + real_tag
        c = constraints.get(full_path)
        if c:
            min_occ, max_occ = c["minOccurs"], c["maxOccurs"]
        else:
            # Fallback: assume optional, unbounded
            min_occ, max_occ = 0, float("inf")

        if child_lower == 'additionalsegments':
            # TODO: check how many additional segments we add
            additional_segments = etree.SubElement(parent_xml, "AdditionalSegments")

            for key, data in additional_segments_tables_simple.items():
                if data_obj.get(key):
                    create_single_segment_for_sigle_fields(additional_segments, data_obj.get(key), key)

            for key, data in additional_segments_tables_mutifield.items():
                if data_obj.get(key):
                    create_single_segment_for_mutifield(additional_segments, data_obj.get(key), key)

            continue
        if child_lower not in data_obj:
            # If it's missing and recognized as a container, produce empty elements if minOccurs > 0
            if child_lower in container_map:
                for _ in range(min_occ):
                    etree.SubElement(parent_xml, real_tag)
            else:
                # Create a single child and recurse
                child_elem = etree.SubElement(parent_xml, real_tag)
                populate_children(child_elem, full_path, data_obj, constraints, name_map, container_map)
            continue

        # If child is present
        value = data_obj[child_lower]
        if isinstance(value, list):
            ct = len(value)
            used = ct if max_occ == float("inf") else min(ct, max_occ)

            for i in range(used):
                item = value[i]
                child_elem = etree.SubElement(parent_xml, real_tag)
                if isinstance(item, dict):
                    populate_children(child_elem, full_path, item, constraints, name_map, container_map)
                else:
                    # Scalar or None
                    child_elem.text = None if (item is None or item == "") else str(item)

            # If fewer items than minOccurs, fill up with empty
            if ct < min_occ:
                for _ in range(min_occ - ct):
                    etree.SubElement(parent_xml, real_tag).text = None

        elif isinstance(value, dict):
            # Single dict -> single child element
            child_elem = etree.SubElement(parent_xml, real_tag)
            populate_children(child_elem, full_path, value, constraints, name_map, container_map)
        else:
            # Scalar
            if not value:
                continue
            child_elem = etree.SubElement(parent_xml, real_tag)
            child_elem.text = None if (value is None or value == "") else str(value)


def _build_single_entity(entity_data, entity_path, constraints, name_map, container_map):
    """
    Builds the <Entity> element as XML string, returning the XML.
    """
    entity_tag = constraints.get(entity_path, {}).get("name", "Entity")
    entity_elem = etree.Element(entity_tag)
    populate_children(entity_elem, entity_path, entity_data, constraints, name_map, container_map)
    # Return as string so we don't try to pickle XML objects
    return etree.tostring(entity_elem, encoding="unicode")


def _build_single_relationship(relationship_data, relationship_path, constraints, name_map, container_map):
    """
    Builds the <Relationship> element as XML string, returning the XML.
    """
    relationship_tag = constraints.get(relationship_path, {}).get("name", "Relationship")
    relationship_elem = etree.Element(relationship_tag)
    populate_children(relationship_elem, relationship_path, relationship_data,
                      constraints, name_map, container_map)
    return etree.tostring(relationship_elem, encoding="unicode")


def build_xml_from_wco_data(wco_data, constraints, name_map, container_map, processes=1):  # cpu_count()):
    """
    Converts wco_data → XML, building large lists (entities, relationships, etc.)
    in parallel without using JSON (uses default Python pickling).
    """
    # Identify root
    root_path_candidates = [p for p in constraints if p.count("/") == 1]
    if root_path_candidates:
        root_path = root_path_candidates[0]
    else:
        root_path = "/Root"

    root_name = constraints.get(root_path, {}).get("name", "Root")
    root_elem = etree.Element(root_name)

    # Make <Entities> container
    entities_path = root_path + "/Entities"
    if entities_path in constraints:
        entities_name = constraints[entities_path]["name"]
    else:
        entities_name = "Entities"
    entities_elem = etree.SubElement(root_elem, entities_name)

    entity_path = entities_path + "/Entity"

    # Make <Relationships> container
    relationships_path = root_path + "/Relationships"
    if relationships_path in constraints:
        relationships_name = constraints[relationships_path]["name"]
    else:
        relationships_name = "Relationships"
    relationships_elem = etree.SubElement(root_elem, relationships_name)

    relationship_path = relationships_path + "/Relationship"

    # Prepare lists
    entities_list = wco_data.get("entities", [])
    relationships_list = wco_data.get("relationships", [])

    # Multiprocess build of Entities
    build_entity_args = [
        (entity_data, entity_path, constraints, name_map, container_map)
        for entity_data in entities_list
    ]
    with Pool(processes=processes) as pool:
        entities_xml_strings = list(
            tqdm(pool.starmap(_build_single_entity, build_entity_args),
                 total=len(build_entity_args),
                 desc="Building Entities in parallel")
        )

    # Attach each <Entity> to the main <Entities> parent
    for entity_str in entities_xml_strings:
        entity_elem = etree.fromstring(entity_str)
        entities_elem.append(entity_elem)

    # Multiprocess build of Relationships
    build_relationship_args = [
        (relationship_data, relationship_path, constraints, name_map, container_map)
        for relationship_data in relationships_list
    ]
    with Pool(processes=processes) as pool:
        relationships_xml_strings = list(
            tqdm(pool.starmap(_build_single_relationship, build_relationship_args),
                 total=len(build_relationship_args),
                 desc="Building Relationships in parallel")
        )

    # Attach each <Relationship> to the main <Relationships> parent
    for relationship_str in relationships_xml_strings:
        relationship_elem = etree.fromstring(relationship_str)
        relationships_elem.append(relationship_elem)

    if 'entitydeletes' in wco_data:
        entitydeletes_list = wco_data.get('entitydeletes', [])
        entitydeletes_path = root_path + "/EntityDeletes"
        entitydeletes_name = "EntityDeletes"
        entitydeletes_elem = etree.SubElement(root_elem, entitydeletes_name)
        entitydelete_path = entitydeletes_path + "/EntityDelete"
        build_entitydelete_args = [
            (entitydelete_data, entitydelete_path, constraints, name_map, container_map)
            for entitydelete_data in entitydeletes_list
        ]
        with Pool(processes=processes) as pool:
            entitydeletes_xml_strings = list(
                tqdm(pool.starmap(_build_single_entity, build_entitydelete_args),
                     total=len(build_entitydelete_args),
                     desc="Building EntityDeletes in parallel")
            )
        for entitydelete_str in entitydeletes_xml_strings:
            entitydelete_elem = etree.fromstring(entitydelete_str)
            entitydeletes_elem.append(entitydelete_elem)
    return root_elem


def write_xml_to_file(xml_root, output_file):
    with open(output_file, 'wb') as f:
        f.write(etree.tostring(
            xml_root,
            pretty_print=True,
            xml_declaration=True,
            encoding='UTF-8'
        ))


def validate_xml(xml_path: str, xsd_file_path: str) -> bool:
    xmlschema_doc = etree.parse(xsd_file_path)
    xmlschema = etree.XMLSchema(xmlschema_doc)
    xml_doc = etree.parse(xml_path)
    result = xmlschema.validate(xml_doc)
    print(xmlschema.error_log)
    return result


def generate_xml_data(data_dir: str, xsd_file_path: str, output_file: str, validate_output_xml: bool = False, mock: bool=False) -> None:
    if mock:
        with open(output_file, 'w') as f:
            data = """<?xml version='1.0' encoding='UTF-8'?>
<WCOData>
    <Entities>
        <Entity>
          <EntityGUID>CD6FEAD6-EBFB-4FAC-BDF1-47E47AA5B2FD</EntityGUID>
        </Entity>
    </Entities>
    <Relationships>
        <Relationship>
          <EntityGUID>5248D129-2092-4017-8E87-A068BD65FF56</EntityGUID>
        </Relationship>
    <EntityDeletes>
        <EntityDelete>
          <EntityGUID>A7B4D9C1-5FB4-41A1-8E22-0D38405C3EC4</EntityGUID>
        </EntityDelete>
    </EntityDeletes>
</WCOData>"""
            f.write(data)
            return
    constraints, name_map = parse_xsd(xsd_file_path=xsd_file_path)
    xsd_element_names = [info['name'] for info in constraints.values()]
    container_map = detect_container_map(constraints=constraints)
    table_names = get_required_parquet_tables(xsd_element_names=xsd_element_names, parquet_map=parquet_map)
    df_map = load_parquet_data_polars_lazy(directory=data_dir, table_names=table_names)
    entitydeletes_df = df_map.pop('entitydeletes', None)
    df = merge_tables(df_map=df_map)
    wco_data = build_wco_data_polars_lazy(df=df, entitydeletes_df=entitydeletes_df)
    xml_root = build_xml_from_wco_data(wco_data=wco_data, constraints=constraints, name_map=name_map,
                                       container_map=container_map)
    write_xml_to_file(xml_root=xml_root, output_file=output_file)

    if validate_output_xml:
        result = validate_xml(xml_path=output_file, xsd_file_path=xsd_file_path)
        print(f"XML validation result: {result}")
