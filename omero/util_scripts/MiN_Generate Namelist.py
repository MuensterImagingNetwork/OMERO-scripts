#!/usr/bin/env python
# -*- coding: utf-8 -*-

import omero.scripts as scripts
from omero.gateway import BlitzGateway
import omero
import re
from omero.rtypes import rstring, rlong, rtime

# main and only function
def generate_namelist(conn, script_params):

    obj_type = script_params["Data_Type"]
    ids = script_params["IDs"]
    objects=[]
    # generate a list of objects from the script parameters
    for id in ids:
        object = conn.getObject(obj_type,id)
        objects.append(object)
    imageCounter=0
    objectCounter=0
    names=[]    # will contain the image names to be printed

    # loop through all objects and get the names from the .listChildren()
    # of the respective object hierarchy
    for obj in objects:

        if obj_type=='Dataset':
            print("Image names for Dataset %s:"%(str(obj.getName())))
            images=list(obj.listChildren())
            for image in images:
                name = image.getName()
                image_id  = image.getID()
                names.append([name,image_id])
                imageCounter=imageCounter+1

        if obj_type=='Plate':
            print("Image names for Plate %s:" % (str(obj.getName())))
            for well in obj.listChildren():
                for wellsample in well.listChildren():
                    img = wellsample.getImage()
                    name = img.getName()
                    image_id = image.getID()
                    names.append([name, image_id])
                    imageCounter = imageCounter + 1

        # a bit convoluted function to sort the names list naturally
        # which is needed for the .csv generation
        # works with only re, which is installed
        convert = lambda text: int(text) if text.isdigit() else text.lower()
        alphanum_key = lambda x: [convert(c) for c in re.split('([0-9]+)', x)]
        names.sort(key=alphanum_key)

        # print loop
        for img in names:
            print(img[0]+" ; "+img[1])
        print("________________________________________________")
        names.clear()   # clear the names list for the nex object
        objectCounter = objectCounter + 1


    return  imageCounter, objectCounter



def run_script():
    data_types = [rstring('Dataset'),rstring('Plate')]

    client = scripts.client(
        'MiN_Generate Namelist.py',
        """
        Generates a list of names of all images in a dataset/plate.
        @author Jens Wendt
        <a href="mailto:jens.wendt@uni-muenster.de">jens.wendt@uni-muenster.de</a>
        """,

        scripts.String(
            "Data_Type", optional=False, grouping="1",
            description="Choose source of images (only Dataset supported)",
            values=data_types, default="Dataset"),

        scripts.List(
            "IDs", optional=False, grouping="2",
            description="List of Dataset IDs").ofType(rlong(0)),
    )

    try:
        script_params = client.getInputs(unwrap=True)
        conn = BlitzGateway(client_obj=client)

        imageCounter, objectCounter  = generate_namelist(conn, script_params)
        message = "Found %d images in %d %ss" %(imageCounter, objectCounter,script_params["Data_Type"])
        client.setOutput("Message", rstring(message))

    finally:
        client.closeSession()


if __name__ == "__main__":
    run_script()