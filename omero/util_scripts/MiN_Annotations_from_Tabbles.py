import omero
from omero.gateway import BlitzGateway
from omero.rtypes import rstring, rlong
import omero.scripts as scripts

import pyodbc
import pandas as pd


# TODO  
#       * optional PATH wenn man Files verschiebt und dann nachträglich den Pfad einpflegen will für OMERO
#       * Wie gehe ich mit dem Hinzufügen von Tabbles-Annotations um wenn schon welche existieren? Append<->Overwrite?
#       * Wie verstecke ich das PWD und die UID?
#       * doppelte Tags umgehen


def annotateObject (conn, script_params, obj, data_dict):
# annotates any object with a given list of KV-pairs as mapAnnotations
    try:
        
        counter_map_ann = 0
        counter_tag_ann = 0
        kv_list_no_mapr =[]

        for ns, key_values in data_dict.items():
            # Namespace should only be None for single tags, with a key "_workspace..."
            if script_params["process_single_tags"]==True and ns==None:
                for key, values in key_values.items():
                    if key.startswith("_"):
                        for value in values:
                            tag_ann = omero.gateway.TagAnnotationWrapper(conn)
                            tag_ann.setValue(value)
                            tag_ann.save()
                            obj.linkAnnotation(tag_ann)
                            counter_tag_ann += 1
                continue
            
            # MapAnnotations (KV-pairs) will have some kind of Namespace
            
            kv_list = []
            # convert the dict of lists into the proper format
            for k, vset in key_values.items():
                for v in vset:
                    kv_list.append([k, v])
            
            if not ns==None:
                # check if we need to set custom NS for OMERO.Mapr 
                if script_params["OMERO.mapr-ready_annotations"]==True and not ns.startswith("_"):
                    map_ann = omero.gateway.MapAnnotationWrapper(conn)
                    map_ann.setNs(ns)
                    map_ann.setValue(kv_list) #expects a list of lists (of strings)
                    map_ann.save()
                    obj.linkAnnotation(map_ann)
                    counter_map_ann = counter_map_ann + len(kv_list)
                # if there is no specific Namespace defined in Tabbles just aggregate all KV pairs into one list
                if ns.startswith("_") or script_params["OMERO.mapr-ready_annotations"]==False:
                    kv_list_no_mapr = kv_list_no_mapr + kv_list
        
        # if any non-specific Namespaces exist create one big MapAnnotation instead of a bunch of smaller ones
        # for better readability
        if len(kv_list_no_mapr)>0:
            map_ann = omero.gateway.MapAnnotationWrapper(conn)
            namespace = omero.constants.metadata.NSCLIENTMAPANNOTATION
            map_ann.setNs(namespace)
            map_ann.setValue(kv_list_no_mapr)
            map_ann.save()
            obj.linkAnnotation(map_ann)
            counter_map_ann = counter_map_ann + len(kv_list_no_mapr)

       
        return True,counter_map_ann, counter_tag_ann
    except Exception as e:
        print(e)
        return False,counter_map_ann, counter_tag_ann


def getData(image):
    path_raw = image.getImportedImageFilePaths()['client_paths'][0]
    path_cleaned = path_raw.replace(";",":") # getting rid of the pesky "C;/../.." path format of OMERO
    path = path_cleaned.replace("/","\\")

    print("path ", path)
    
    # variables for the SQL Server connection
    driver="{ODBC Driver 17 for SQL Server}"
    ip="10.15.127.4,49650"
    database="tabbles4"

    # connect to tabbles DB
    cnxn_str = ('DRIVER='+driver+';SERVER='+ip+';DATABASE='+database+';UID=python;PWD=python')
    cnxn = pyodbc.connect(cnxn_str)
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
    print("querystring ",query)

    # Execute the query
    cursor.execute(query)

    # get all the lines
    raw_list = cursor.fetchall()
    # raw_list order: name, value, key

    # # loop through and sanitize the path-strings into the file names
    # for x in raw_list:
    #     x[0] = x[0][x[0].rindex("\\")+1:]
    
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
        
        # Create nested dictionaries if they don't exist
        if namespace not in kv_data:
            kv_data[namespace] = {}
        if key not in kv_data[namespace]:
            kv_data[namespace][key] = []
        
        # Append the value to the list of values
        kv_data[namespace][key].append(value)
        
    return kv_data
    
def getImages(conn, script_params):
    if script_params["Data_Type"]=="Dataset":
        datasets = conn.getObjects("Dataset",script_params["IDs"])
        images = []
        for ds in datasets:
            dataset_images = list(ds.listChildren())
            for img in dataset_images:
                images.append(img)
                
    elif script_params["Data_Type"]=="Image":
        images = [conn.getObjects("Image",script_params["IDs"])]

    elif script_params["Data_Type"]=="Project":
        projects = conn.getObjects("Project",script_params["IDs"])
        datasets = []
        for project in projects:
            project_datasets = list(project.listChildren())
            for ds in project_datasets:
                datasets.append(ds)
        images = []
        for ds in datasets:
            dataset_images = list(ds.listChildren())
            for img in dataset_images:
                images.append(img)

    return images
 

def tabbles_annotation(conn, script_params):
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
            data = getData(image)
        print("got data from SQL")
        print("data_dict: ",data)
        annotated, kv_annotated, tags_annotated = annotateObject (conn, script_params, image, data)
        sum_KV += kv_annotated
        sum_tag += tags_annotated
        last_path = path
        if annotated: 
            number_of_images+=1
            print("annotated object")
            print("###########################")


    return number_of_images, total_images, sum_KV, sum_tag
   

def run_script():

    data_types = [rstring('Dataset'), rstring('Image'), rstring('Project')]
    client = scripts.client(
        'Get_Annotations_from_Tabbles',
        """
    This script connects to the Tabbles Database and gets tags for the original files of the selected Images.
    Then it converts them to Key-Value pairs and Tags.
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
            "process_single_tags", optional=True, grouping="4",
            description="Allows to create OMERO Tags from single Tabble Tags without Parents",
            default=False),

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

        #################
        # MAIN FUNCTION #
        number_of_images, total_images, sum_kv, sum_tags = tabbles_annotation(conn, script_params)
        #################


        message = f"Annotated {number_of_images}/{total_images} images with {sum_kv} total Key-Value pairs and {sum_tags} total Tags."
        client.setOutput("Message", rstring(message))

    finally:
        client.closeSession()


if __name__ == "__main__":
    run_script()