import omero
from omero.gateway import BlitzGateway
from omero.rtypes import rstring, rlong
import omero.scripts as scripts
#from collections import OrderedDict
from omero.cmd import Delete2

import pyodbc
import json
import pandas as pd

DEFAULT_NAMESPACE = omero.constants.metadata.NSCLIENTMAPANNOTATION


# TODO  
#       * optional PATH if you move Image Files and want to change Annotations later on with new Tabbles Annotations
#       * Documentation verbessern
#       * nomenclature der variablen namen vereinheitlichen

def get_existing_map_annotations(obj):
    """Get all Map Annotations linked to the object"""
    existing = {}
    for ann in obj.listAnnotations():
        if isinstance(ann, omero.gateway.MapAnnotationWrapper):
            ns = ann.getNs()
            if ns not in existing:
                existing[ns] = {}
            kvs = ann.getValue()
            for k, v in kvs:
                if k not in existing[ns]:
                    existing[ns][k] = []
                existing[ns][k].append(v)
    return existing

def get_tag_dict(conn):
    """Gets a dict of all existing Tag Names with their respective OMERO IDs as values"""
    meta = conn.getMetadataService()
    taglist = meta.loadSpecifiedAnnotations("TagAnnotation","","",None)
    tag_dict = {}
    for tag in taglist:
        name = tag.getTextValue().getValue()
        tag_id = tag.getId().getValue()
        if name not in tag_dict:
            tag_dict[name] = tag_id
    return tag_dict

def get_linked_tag_annotations(obj):
    """Get all Tag Annotations linked to the object"""
    existing = []
    for ann in obj.listAnnotations():
        if isinstance(ann, omero.gateway.TagAnnotationWrapper):
            value = ann.getValue()
            existing.append(value)
    return existing

def remove_map_annotations(conn, object, ns):
    """Remove MapAnnotations matching a given Namespace from the object"""
    anns = list(object.listAnnotations())
    mapann_ids = [ann.id for ann in anns
                  if isinstance(ann, omero.gateway.MapAnnotationWrapper) and ann.getNs()==ns]

    try:
        delete = Delete2(targetObjects={'MapAnnotation': mapann_ids})
        handle = conn.c.sf.submit(delete)
        conn.c.waitOnCmd(handle, loops=10, ms=500, failonerror=True,
                         failontimeout=False, closehandle=False)

    except Exception as ex:
        print("Failed to delete links: {}".format(ex.message))
    return

def remove_tag_annotations(conn, object):
    """Remove ALL Tag Annotations on the object and returns the number of deleted Tag Annotations"""
    anns = list(object.listAnnotations())
    tagann_ids = [ann.id for ann in anns
                  if isinstance(ann, omero.gateway.TagAnnotationWrapper)]
    
    number_of_deleted_tags = len(tagann_ids)

    try:
        delete = Delete2(targetObjects={'TagAnnotation': tagann_ids})
        handle = conn.c.sf.submit(delete)
        conn.c.waitOnCmd(handle, loops=10, ms=500, failonerror=True,
                         failontimeout=False, closehandle=False)

    except Exception as ex:
        print("Failed to delete links: {}".format(ex.message))
    return number_of_deleted_tags

def getImages(conn, script_params):
    '''Gets a list of all Images from a (list) of Project(s)/Dataset(s)/Image(s)'''
    images = []
    
    if script_params["Data_Type"]=="Dataset":
        datasets = conn.getObjects("Dataset",script_params["IDs"])
        for ds in datasets:
            dataset_images = list(ds.listChildren())
            for img in dataset_images:
                images.append(img)
                
    elif script_params["Data_Type"]=="Image":
        for id in script_params["IDs"]:
            img = conn.getObject("Image",id)
            images.append(img)

    elif script_params["Data_Type"]=="Project":
        projects = conn.getObjects("Project",script_params["IDs"])
        datasets = []
        for project in projects:
            project_datasets = list(project.listChildren())
            for ds in project_datasets:
                datasets.append(ds)
        for ds in datasets:
            dataset_images = list(ds.listChildren())
            for img in dataset_images:
                images.append(img)

    return images

def getData(image):
    '''Gets Data for one Image from the Microsoft SQL Server that Tabbles uses'''
    path_raw = image.getImportedImageFilePaths()['client_paths'][0]
    path_cleaned = path_raw.replace(";",":") # getting rid of the pesky "C;/../.." path format of OMERO
    path = path_cleaned.replace("/","\\")

    # connect to tabbles DB
    cnxn_str = ('DRIVER='+DRIVER+';SERVER='+SERVER+';DATABASE='+DATABASE+';UID='+USERNAME+';PWD='+PWD)
    cnxn = pyodbc.connect(cnxn_str)
    print("connected to MSSQL server")
    cursor = cnxn.cursor()

    # the raw SQL query including namespaces
    query=(r"""
    SELECT distinct TAG3.name as namespace_, TAG2.name as key_, TAG1.name as value_
    FROM [tabbles4].[dbo].[file2] files
    INNER JOIN [tabbles4].[dbo].[taggable_has_tag]
    ON (files.[idTaggable] = [tabbles4].[dbo].[taggable_has_tag].id_taggable)
    INNER JOIN [tabbles4].[dbo].[tag] TAG1
    ON (TAG1.id = [tabbles4].[dbo].[taggable_has_tag].id_tag)
    LEFT JOIN [tabbles4].[dbo].[tabble_is_child_of_tag_for_user] CHILD1
    ON (CHILD1.id_tabble_child=TAG1.id_tabble)
    LEFT JOIN [tabbles4].[dbo].[tag] TAG2
    ON (TAG2.id=CHILD1.id_tag_parent)
    LEFT JOIN [tabbles4].[dbo].[tabble_is_child_of_tag_for_user] CHILD2
    ON (CHILD2.id_tabble_child=TAG2.id_tabble)
    LEFT JOIN [tabbles4].[dbo].[tag] TAG3
    ON (TAG3.id=CHILD2.id_tag_parent)
    WHERE files.path LIKE '{}'
    """.format(path))
    print("querystring ends with ",query[query.index("LIKE"):])

    # Execute the query
    cursor.execute(query)

    # get all the lines
    raw_list = cursor.fetchall()
   
    # create a pd.df from the raw_list to get rid of duplicate entries (when namespace/parent tag = "_workspace...")
    raw_df = pd.DataFrame.from_records(raw_list)
    duplicates_removed_df = raw_df.drop_duplicates(subset=[1,2],keep="last") #"last" as the namespaces are ordered by alphabet

    # generate a dict of image-names with dicts of namespaces with dicts of KV-pairs, with the values as ";"-separated String
    # Iterate over each row in the dataframe
    kv_data={}
    for _, row in duplicates_removed_df.iterrows():
        namespace = row[0]
        key = row[1]
        value = row[2]
        # Create OMERO.mapr specific Namespaces
        if namespace==None or namespace.startswith("_"):
            new_namespace = namespace
        else: new_namespace = "mapr_"+namespace

        # Create nested dictionaries if they don't exist
        if new_namespace not in kv_data:
            kv_data[new_namespace] = {}
        if key not in kv_data[new_namespace]:
            kv_data[new_namespace][key] = []
        # Append the value to the list of values
        kv_data[new_namespace][key].append(value)
        
    return kv_data

def split_data(script_params, data_dict):
    '''Splits the data dictionary that comes from Tabbles into the relevant formats for MapAnnotations and TagAnnotations
    respecting the script parameters.
    '''
    new_tags = []
    new_KVpairs_list = []  # as simple list of list, in the matching format for annotation
    new_KVpairs_dict = {}  # as dict of dicts of lists, matching the data_dict format from Tabbles

    # get lists/dicts for KV-pairs and Tags out of the data from Tabbles
    for ns, key_values in data_dict.items():
        # TAGS 
        # Namespace should only be None for single tags, with a key "_workspace..."
        if script_params["Process_single_tags"]==True and ns==None:
            for key, values in key_values.items():
                # double-check if it is really a singular Tabbles Tag, as only those
                # have "system" parent tags starting with "_..."
                if key.startswith("_"): 
                    for v in values:
                        new_tags.append(v)

        # KV-PAIRS
        # MapAnnotations wil have a not None Namespace
        if not ns==None:
            if script_params["OMERO.mapr-ready_annotations"]==False:
            # create a list of lists, disregard Namespaces
                for key, values in key_values.items():
                    # double-check
                    assert key.startswith("_")==False, f"Problem with {ns}:{key}:{values}"
                    for v in values:
                        # check if the KV-pair already exists (could be possible if 
                        # the same KV-pair exists in different NS)
                        if not [key,v] in new_KVpairs_list:
                            new_KVpairs_list.append([key,v])

            if script_params["OMERO.mapr-ready_annotations"]==True:
            # create a dict of dicts of lists respecting Namespaces
                # check if specific parent Namespace in Tabbles exists, if not set it as OMERO default
                if ns.startswith("_"):
                    namespace = DEFAULT_NAMESPACE
                else: namespace = ns

                if namespace not in new_KVpairs_dict:
                    new_KVpairs_dict[namespace] = {}
                for key, values in key_values.items():
                    if key not in new_KVpairs_dict[namespace]:
                        new_KVpairs_dict[namespace][key] = []
                    for v in values:
                        if v not in new_KVpairs_dict[namespace][key]:
                            new_KVpairs_dict[namespace][key].append(v)
    
    return new_tags, new_KVpairs_list, new_KVpairs_dict


def annotateObject (conn, script_params, obj, data_dict):
    """annotates any object with a given data dictionary from Tabbles as MapAnnotations or TagAnnotations"""
    
    counter_map_ann = 0
    counter_tag_ann = 0

    # get the data into fitting formats
    new_tags, new_KVpairs_list, new_KVpairs_dict = split_data(script_params, data_dict)
    

    # decide what to do with the new KV-Pairs/Tags
        
    if script_params["What_to_do_with_existing_Annotations"]=="Overwrite":
        # TAGS
        # delete all old tags, check if the new Tags exists at all and create it if not, link all new Tags
        
        if len(new_tags)>0:
            count_removed = remove_tag_annotations(conn,obj)
            tag_dict = get_tag_dict(conn)
            counter = 0
            for tag_value in new_tags:
                if tag_value not in tag_dict:
                    tag_ann = omero.gateway.TagAnnotationWrapper(conn)
                    tag_ann.setValue(tag_value)
                    tag_ann.save()
                    obj.linkAnnotation(tag_ann)
                    print(f"created new Tag '{tag_value}'.")
                    counter += 1
                else:
                    tag_ann = conn.getObject("TagAnnotation",tag_dict[tag_value])
                    obj.linkAnnotation(tag_ann)
                    counter += 1
            counter_tag_ann = counter - count_removed
        
        # KV-PAIRS
        # without OMERO.Mapr
        if len(new_KVpairs_list)>0:
            assert script_params["OMERO.mapr-ready_annotations"]==False, "something went wrong with OMERO.Mapr Parameter"
            namespace = DEFAULT_NAMESPACE
            # get all existing KV-pairs with the default Namespace
            existing_map_annotations_lists = []
            # check if KV-pairs with the default Namespace exist and retrieve them
            existing_map_annotations = get_existing_map_annotations(obj)
            if len(existing_map_annotations)>0:
                if namespace in existing_map_annotations:
                    for k, values in get_existing_map_annotations(obj)[namespace].items():
                        for v in values:
                            existing_map_annotations_lists.append([k,v])
            # add the new KV-pairs to them
            combined_KV_list = existing_map_annotations_lists + new_KVpairs_list
            # and remove the old ones
            remove_map_annotations(conn, obj, namespace)
            # then annotate the combined list
            map_ann = omero.gateway.MapAnnotationWrapper(conn)
            map_ann.setNs(namespace)
            map_ann.setValue(combined_KV_list) #expects a list of lists (of two strings, e.g. [[key1,value1][key1,value2]])
            map_ann.save()
            obj.linkAnnotation(map_ann)
            counter_map_ann = counter_map_ann + len(new_KVpairs_list)
        
        # with OMERO.mapr
        if len(new_KVpairs_dict)>0:
            assert script_params["OMERO.mapr-ready_annotations"]==True, "something went wrong with OMERO.Mapr Parameter"
            # check if Mapr Annotations exist that do not show up in 
            # the changed data --> were deleted; if so, remove them
            existing_map_annotations = get_existing_map_annotations(obj)
            deleted = 0
            added = 0
            if len(existing_map_annotations)>0:
                for ns in existing_map_annotations.keys():
                    if ns.startswith("mapr_") and ns not in new_KVpairs_dict:
                        deleted = deleted + len(existing_map_annotations[ns])
                        remove_map_annotations(conn, obj, ns)

            for namespace, kv in new_KVpairs_dict.items():
                deleted = deleted + len(existing_map_annotations[namespace])
                remove_map_annotations(conn, obj, namespace)
                map_ann = omero.gateway.MapAnnotationWrapper(conn)
                map_ann.setNs(namespace)
                kv_list = []
                for k,values in kv.items():
                    for v in values:
                        kv_list.append([k,v])
                map_ann.setValue(kv_list)
                map_ann.save()
                obj.linkAnnotation(map_ann)
                added = added + len(new_KVpairs_dict)
            counter_map_ann = added - deleted


    if script_params["What_to_do_with_existing_Annotations"]=="Append":
        # TAGS
        # check if the new Tags are already linked, if the Tag exists at all and create it if not
        if len(new_tags)>0:
            existing_tags = get_linked_tag_annotations(obj)
            tag_dict = get_tag_dict(conn)
            for tag in new_tags:
                if tag not in existing_tags:
                    if tag not in tag_dict:
                        tag_ann = omero.gateway.TagAnnotationWrapper(conn)
                        tag_ann.setValue(tag)
                        tag_ann.save()
                        obj.linkAnnotation(tag_ann)
                        print(f"created a new Tag '{tag}'.")
                        counter_tag_ann += 1
                    else:
                        tag_ann = conn.getObject("TagAnnotation",tag_dict[tag])
                        obj.linkAnnotation(tag_ann)
                        counter_tag_ann += 1

        # KV-PAIRS
        # With OMERO.Mapr
        if len(new_KVpairs_dict)>0:
            assert script_params["OMERO.mapr-ready_annotations"]==True, "something went wrong with OMERO.Mapr Parameter"
            existing_kvPairs = get_existing_map_annotations(obj)
            # check if the new KV-pairs contain anything that is not already existing and
            # append it to the existing ones if so
            for namespace, kv in new_KVpairs_dict.items():
                if namespace not in existing_kvPairs:
                    existing_kvPairs[namespace] = {}
                for k,values in kv.items():
                    if k not in existing_kvPairs[namespace]:
                        existing_kvPairs[namespace][k] = []
                    for v in values:
                        if v not in existing_kvPairs[namespace][k]:
                            existing_kvPairs[namespace][k].append(v)
                            counter_map_ann += 1
            
            # annotate the combined dict
            for namespace, kv in existing_kvPairs.items():
                remove_map_annotations(conn, obj, namespace)
                map_ann = omero.gateway.MapAnnotationWrapper(conn)
                map_ann.setNs(namespace)
                kv_pairs_to_append = []
                for k, values in kv.items():
                    for v in values:
                        kv_pairs_to_append.append([k,v])
                map_ann.setValue(kv_pairs_to_append)
                map_ann.save()
                obj.linkAnnotation(map_ann)


        # Without OMERO.Mapr
        if len(new_KVpairs_list)>0:
            assert script_params["OMERO.mapr-ready_annotations"]==False, "something went wrong with OMERO.Mapr Parameter"
            existing_kvPairs = get_existing_map_annotations(obj)
            # change the format into a list of lists
            updated_list = []
            if DEFAULT_NAMESPACE in existing_kvPairs:
                for k, values in existing_kvPairs[DEFAULT_NAMESPACE].items():
                    for v in values:
                        updated_list.append([k,v])
            
            # check if the new list contains items that have to be appended to the existing KV-pairs
            for kv in new_KVpairs_list:
                if kv not in updated_list:
                    updated_list.append(kv)
                    counter_map_ann += 1

            # remove the old MapAnnotations and annotate the updated list
            remove_map_annotations(conn, obj, DEFAULT_NAMESPACE)
            map_ann = omero.gateway.MapAnnotationWrapper(conn)
            map_ann.setNs(DEFAULT_NAMESPACE)
            map_ann.setValue(updated_list)
            map_ann.save()
            obj.linkAnnotation(map_ann)


    return counter_map_ann, counter_tag_ann

def tabbles_annotation(conn, script_params):
    '''Main function'''
    # get a list of all images
    images = getImages(conn, script_params)
    total_images = len(images)

    number_of_images = 0
    sum_KV = 0
    sum_tag = 0
    

    last_path = ""
    for image in images:
        print("processing ",image.getName())
        path = image.getImportedImageFilePaths()['client_paths'][0]
        if path != last_path:
            # get the data via SQL Query from the Tabbles Database
            data = getData(image)
            print("got data from SQL")
        else: print("used data from last image")
        print("data_dict: ",data)
        # for each image, pass the data from Tabbles, check the script paramters and
        # annotate accordingly MapAnnotations/TagAnnotations
        kv_annotated, tags_annotated = annotateObject (conn, script_params, image, data)
        sum_KV += kv_annotated
        sum_tag += tags_annotated
        last_path = path
        if kv_annotated!=0 or tags_annotated!=0: 
            number_of_images+=1
            print("succesfully annotated image")
            print("###########################")


    return number_of_images, total_images, sum_KV, sum_tag
   

def run_script():

    data_types = [rstring('Dataset'), rstring('Image'), rstring('Project')]
    existing_kv = [rstring('Append'), rstring('Overwrite')]
    client = scripts.client(
        'Get_Annotations_from_Tabbles',
        """
    This script connects to the Tabbles Database and gets Tabbles-Tags for the original files of the selected Images.
    Then it converts them to Key-Value pairs and Tags in OMERO.
    Namespaces used for OMERO.mapr must be configured with the prefix 'mapr_' in the omero web config
    (e.g. "ns": ["mapr_<Namespace>"] in OMERO relates to the <Namespace> Tag in Tabbles).
    For MapAnnotations the script supports a 2 Tier hierarchy of Tabbles Tags, e.g.:
    Namespace-Tag
    |
    |-- Key-Tag
    |......|-- Value-Tag
    |......|-- Value2-Tag
    |
    |-- Key2-Tag

        """,
        scripts.String(
            "Data_Type", optional=False, grouping="1",
            description="Choose source of images",
            values=data_types, default="Dataset"),

        scripts.List(
            "IDs", optional=False, grouping="2",
            description="single ID or comma separated list of IDs").ofType(rlong(0)),

        scripts.Bool(
            "OMERO.mapr-ready_annotations", optional=True, grouping="3",
            description="Allows the use of NAMESPACES to create OMERO.mapr-compatible Key-Value 'paragraphs'",
            default=False),

        scripts.Bool(
            "Process_single_tags", optional=True, grouping="4",
            description="Allows to create OMERO Tags from single Tabble Tags without Parents",
            default=False),

        scripts.String(
            "What_to_do_with_existing_Annotations", optional=False, grouping="5",
            description="If Key-Value pairs (with this Namespace) exist, the"
            " existing KV-pairs can be removed and only the new ones get added with the option 'Overwrite'.\n"
            "The option 'Append' will compare existing KV-pairs (of a Namespace) with the set of proposed new ones and append only the non-existing new KV-pairs.\n"
            "The same logic will apply for Tags.",
            values=existing_kv, default="Overwrite"),

        authors=["Jens Wendt"],
        institutions=["Imaging Network, Uni Muenster"],
        contact="https://forum.image.sc/tag/omero"
    )

    try:
        # process the list of args above.
        script_params = {}
        for key in client.getInputKeys():
            if client.getInput(key):
                script_params[key] = client.getInput(key, unwrap=True)

        # wrap client to use the Blitz Gateway
        conn = BlitzGateway(client_obj=client)
        
        # get login parameters for the Microsoft SQL Server from a .json on the OMERO server
        global DRIVER,SERVER,DATABASE,USERNAME,PWD
        with open('/opt/omero/MSSQL_login.json', 'r') as fp:
            data = json.load(fp)
        DRIVER = data["DRIVER"]
        SERVER = data["SERVER"]
        DATABASE = data["DATABASE"]
        USERNAME = data["UID"]
        PWD = data["PWD"]

        #################
        # MAIN FUNCTION #
        number_of_images, total_images, sum_kv, sum_tags = tabbles_annotation(conn, script_params)
        #################

        if sum_tags>=0 and sum_kv>=0:
            message = f"Annotated {number_of_images}/{total_images} images. Appended {sum_kv} total Key-Value pairs and {sum_tags} total Tags."
        elif sum_tags>=0 and sum_kv<0:
            message = f"Annotated {number_of_images}/{total_images} images. Removed {abs(sum_kv)} total Key-Value pairs and appended {sum_tags} total Tags."
        elif sum_tags<0 and sum_kv>=0:
            message = f"Annotated {number_of_images}/{total_images} images. Appended {sum_kv} total Key-Value pairs and removed {abs(sum_tags)} total Tags."
        elif sum_tags<0 and sum_kv<0:
            message = f"Annotated {number_of_images}/{total_images} images. Removed {abs(sum_kv)} total Key-Value pairs and removed {abs(sum_tags)} total Tags."
        
        client.setOutput("Message", rstring(message))

    finally:
        client.closeSession()

if __name__ == "__main__":
    run_script()