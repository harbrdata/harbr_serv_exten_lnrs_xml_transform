import gc
import json
import os
from collections import defaultdict
from multiprocessing import Pool
from multiprocessing import cpu_count

import polars as pl
import psutil
from lxml import etree
from tqdm import tqdm

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


def load_parquet_data_polars_lazy(directory: str):
    """
    Returns a dict of {table_name: LazyFrame}, scanning parquet files in each subfolder.
    """
    dataframes = {}
    print("load data")
    for name in ['entity', 'entity_element_details_consolidated', "entitydeletes",
                 "custom_feed_entity_match_type_lookup"]:
        folder = os.path.join(directory, name)
        if os.path.isdir(folder):
            parquet_files = [
                os.path.join(folder, f)
                for f in os.listdir(folder)
            ]
            if parquet_files:
                # Create a single LazyFrame by concatenating multiple scans
                lazy_scans = [pl.scan_parquet(x) for x in parquet_files]
                dataframes[name] = pl.concat(lazy_scans, how="vertical")
    return dataframes


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


def populate_children(parent_xml, parent_path, data_obj, constraints, name_map, container_map, all_data):
    """
    Recursively populates children of parent_xml using data_obj.
    The logic follows the original approach, but factored out at top-level
    so that it's pickleable for multiprocessing.
    """
    known_map = name_map.get(parent_path, {})
    try:
        for child_lower, real_tag in known_map.items():
            full_path = parent_path + "/" + real_tag
            c = constraints.get(full_path)
            if c:
                min_occ, max_occ = c["minOccurs"], c["maxOccurs"]
            else:
                # Fallback: assume optional, unbounded
                min_occ, max_occ = 0, float("inf")

            # Example specialized conditions
            if child_lower == 'entityaddress' or child_lower == 'entityaddresses':
                pass  # Example placeholder

            if child_lower == 'additionalsegments':
                # Some specialized logic
                etree.SubElement(parent_xml, "AdditionalSegments")
                # (Placeholder for your specific usage)
                continue

            # If child_lower not in data_obj
            if child_lower not in data_obj:
                # If it's missing but recognized as a container type,
                # produce empty elements if minOccurs > 0
                if child_lower in container_map:
                    for _ in range(min_occ):
                        etree.SubElement(parent_xml, real_tag)
                else:
                    # Create a single child and recurse
                    child_elem = etree.SubElement(parent_xml, real_tag)
                    populate_children(child_elem, full_path, data_obj, constraints, name_map, container_map, all_data)
                continue

            # Child is present
            value = None
            # Try "all_data" first
            if child_lower in all_data:
                value = all_data.get(child_lower)
                if value:
                    try:
                        # Attempt to parse JSON
                        value = json.loads(value)
                        a = []
                        for i in value:
                            if isinstance(i, list):
                                a.extend(i)
                            else:
                                a.append(i)
                    except Exception:
                        pass

            # If not found or is falsy, use data_obj
            if not value and child_lower in data_obj:
                value = data_obj[child_lower]

            try:
                if isinstance(value, list):
                    ct = len(value)
                    used = ct if max_occ == float("inf") else min(ct, max_occ)
                    for i in range(used):
                        item = value[i]
                        child_elem = etree.SubElement(parent_xml, real_tag)
                        if isinstance(item, dict):
                            populate_children(child_elem, full_path, item, constraints, name_map, container_map,
                                              all_data)
                        else:
                            child_elem.text = None if (item is None or item == "") else str(item)
                    # If fewer items than min_occ, fill up with empty ones
                    if ct < min_occ:
                        for _ in range(min_occ - ct):
                            etree.SubElement(parent_xml, real_tag).text = None

                elif isinstance(value, dict):
                    # Single dict => single child element
                    child_elem = etree.SubElement(parent_xml, real_tag)
                    populate_children(child_elem, full_path, value, constraints, name_map, container_map, all_data)
                else:
                    # Scalar or no value
                    if not value:
                        continue
                    child_elem = etree.SubElement(parent_xml, real_tag)
                    child_elem.text = None if (value is None or value == "") else str(value)

            except Exception:
                pass

    except Exception:
        pass


def _build_single_entity(data: dict, entity_path: str, constraints: dict, name_map: dict, container_map: dict):
    """
    Builds the <Entity> element as XML string, returning the XML.
    Expects a dictionary 'data' that has all the needed fields.
    """
    entity_tag = constraints.get(entity_path, {}).get("name", "Entity")
    entity_elem = etree.Element(entity_tag)
    # Pass data as both data and all_data
    populate_children(entity_elem, entity_path, data, constraints, name_map, container_map, data)
    return etree.tostring(entity_elem, encoding="unicode")


def build_xml_from_wco_data(df_map, constraints, name_map, container_map, processes=cpu_count(), output_file=None):
    """
    Generates XML from wco_data in chunked mode, attempting to keep memory usage lower by:
      • Using smaller chunks (e.g., 2500).
      • Removing the pivot step, instead grouping "element→value" pairs in a more compact way.
      • Writing results incrementally to a file instead of storing a massive XML structure in memory.
      • Deleting references and calling gc.collect(streaming=True) after each chunk.
    """
    # 1) Identify root path & containers
    root_path_candidates = [p for p in constraints if p.count("/") == 1]
    if root_path_candidates:
        root_path = root_path_candidates[0]
    else:
        root_path = "/Root"
    root_name = constraints.get(root_path, {}).get("name", "Root")

    # Paths for Entities container
    entities_path = root_path + "/Entities"
    entities_name = constraints.get(entities_path, {}).get("name", "Entities")
    entity_path = entities_path + "/Entity"

    # 2) Retrieve DataFrames & filter relevant GUIDs once
    all_data = df_map["entity"]
    data = df_map["entity_element_details_consolidated"]
    custom_feed_entity_match_type_lookup = df_map["custom_feed_entity_match_type_lookup"]

    # Filter out IDs we care about
    filtered_ids = (
        custom_feed_entity_match_type_lookup
        .filter(pl.col("entity_match_type") == "matched_entity")
        .select("entityguid")
        .collect(streaming=True)
    )
    filtered_guid_list = filtered_ids["entityguid"].to_list()

    # Filter main entity data to relevant ones
    entities = all_data.filter(pl.col("entityguid").is_in(filtered_guid_list))
    len_entities_table = entities.select(pl.count()).collect(streaming=True).item()

    # 3) Prepare an output XML file. We'll do incremental writes.
    # Adjust chunk_size as needed
    chunk_size = 25000

    with open(output_file, "wb") as f_out:
        # Write the <Root> and <Entities> open tags
        f_out.write(f"<{root_name}>\n".encode("utf-8"))
        f_out.write(f"  <{entities_name}>\n".encode("utf-8"))

        for i in range(0, len_entities_table, chunk_size):
            proc = psutil.Process(os.getpid())
            print(
                f"[Chunk {i}..{i + chunk_size}] Mem before aggregator slice: {proc.memory_info().rss / 1024 ** 2:.2f} MB")
            from pdb import set_trace
            # (A) Build a lazy aggregator for "element" -> "value" strings
            aggregator_df = entities.lazy().slice(i, chunk_size).join(data.lazy(), on="entityguid", how="inner").with_columns(pl.col("element").str.to_lowercase().alias("element")).group_by(["entityguid", "element"]).agg(pl.col("value").str.concat(", ")).collect()

            aggregator_df.shrink_to_fit()

            print(
                f"[Chunk {i}..{i + chunk_size}] Mem after aggregator collect: {proc.memory_info().rss / 1024 ** 2:.2f} MB")

            # (C) Group by entityguid into a list of (element, value) pairs
            aggregator_gb = (
                aggregator_df
                .group_by("entityguid")
                .agg(
                    [
                        pl.struct(["element", "value"]).alias("pairs")
                    ]
                )
            )
            aggregator_gb.shrink_to_fit()
            print(
                f"[Chunk {i}..{i + chunk_size}] Mem after grouping pairs: {proc.memory_info().rss / 1024 ** 2:.2f} MB")

            # (D) Fetch the chunk of main entity rows
            chunk_df = entities.lazy().slice(i, chunk_size).collect(streaming=True)

            # (E) Join so each chunk row has the "pairs" list
            result_df = chunk_df.join(aggregator_gb, on="entityguid", how="left")
            result_df.shrink_to_fit()
            print(
                f"[Chunk {i}..{i + chunk_size}] Mem after join aggregator: {proc.memory_info().rss / 1024 ** 2:.2f} MB")

            # Prepare arguments for parallel entity-building
            build_entity_args = []
            for row in result_df.iter_rows(named=True):
                # Turn row into a dictionary
                row_dict = dict(row)
                # Incorporate the pairs into key->value
                if "pairs" in row_dict and row_dict["pairs"] is not None:
                    for pair in row_dict["pairs"]:
                        element_key = pair["element"]
                        value_str = pair["value"]
                        # If needed, parse or split value_str
                        # row_dict[element_key] = value_str.split(", ")
                        row_dict[element_key] = value_str
                # Remove "pairs"
                if "pairs" in row_dict:
                    del row_dict["pairs"]

                build_entity_args.append(
                    (row_dict, entity_path, constraints, name_map, container_map)
                )

            # (F) Build XML in parallel
            with Pool(processes=processes) as pool:
                entities_xml_strings = list(
                    tqdm(
                        pool.starmap(_build_single_entity, build_entity_args),
                        total=len(build_entity_args),
                        desc=f"Building Entities {i}..{i + chunk_size}"
                    )
                )

            # (G) Write each entity's XML to the file incrementally
            for entity_el_str in entities_xml_strings:
                entity_str_indented = "    " + entity_el_str.strip() + "\n"
                f_out.write(entity_str_indented.encode("utf-8"))

            # Clean up references, run GC
            del aggregator_df, aggregator_gb
            del chunk_df, result_df, build_entity_args, entities_xml_strings
            gc.collect()

            proc = psutil.Process(os.getpid())
            print(f"[Chunk {i}..{i + chunk_size}] Mem after cleanup: {proc.memory_info().rss / 1024 ** 2:.2f} MB")
            print("-" * 70)

        # Close the Entities & Root tags
        f_out.write(f"  </{entities_name}>\n".encode("utf-8"))
        f_out.write(f"</{root_name}>\n".encode("utf-8"))

    print(f"XML output written incrementally to {output_file}")
    return None


def validate_xml(xml_path: str, xsd_file_path: str) -> bool:
    xmlschema_doc = etree.parse(xsd_file_path)
    xmlschema = etree.XMLSchema(xmlschema_doc)
    xml_doc = etree.parse(xml_path)
    result = xmlschema.validate(xml_doc)
    print(xmlschema.error_log)
    return result


def generate_xml_data(data_dir: str, output_file: str, xsd_file_path: str = "/app/schema.xsd", validate_output_xml: bool = False, mock: bool = False) -> None:
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
    container_map = detect_container_map(constraints=constraints)
    df_map = load_parquet_data_polars_lazy(directory=data_dir)

    import psutil
    process = psutil.Process(os.getpid())
    print(f"Memory usage before loading parquet data: {process.memory_info().rss / 1024 ** 2} MB")

    build_xml_from_wco_data(df_map=df_map, constraints=constraints, name_map=name_map,
                            container_map=container_map, output_file=output_file)

    if validate_output_xml:
        result = validate_xml(xml_path=output_file, xsd_file_path=xsd_file_path)
        print(f"XML validation result: {result}")
