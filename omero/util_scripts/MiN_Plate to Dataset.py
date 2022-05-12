#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

This script converts Plates to Datasets.
All images are linked to a new Dataset and - if desired -
the link to the original Plate is cut.

@author Jens Wendt
<a href="mailto:jens.wendt@uni-muenster.de">jens.wendt@uni-muenster.de</a>
@version 1
"""

import omero.scripts as scripts
from omero.gateway import BlitzGateway
import omero
import ezomero as ez
import re   # for sorting of list of images

from omero.rtypes import rint, rlong, rstring, robject, unwrap


def plate_to_dataset (conn, script_params)

    id = script_params["Plate_ID"]
    plate = conn.getObject("Plate",id)
    if plate is None:
       message = "No plate with this id found"
       return message

    # loop through the plate/well/wellsample and generate a list of all images (ids)
    image_list = []
    for well in plate.listChildren():
        for wellsample in well.listChildren():
            image = wellsample.getImage()
            image_list.append(image)
    image_ids= [i.getId() for i in image_list]

    # create a new Dataset
    if "Dataset_name" in script_params and len(script_params["Dataset_name"]) > 0:
        dataset_name = script_params["Dataset_name"]
    else:
        dataset_name = plate.getName()
    dataset_id = ez.post_dataset(conn, dataset_name)
    new_obj = conn.getObject("Dataset",dataset_id)

    # link the images to the new Dataset
    ez.link_images_to_dataset(conn, image_ids, dataset_id)

    message = "Linked %d image(s) to the new Dataset %s" % len(image_list),dataset_name


    return new_obj, message


def run_script():
    """
    The main entry point of the script, as called by the client via the
    scripting service, passing the required parameters.
    """


    client = scripts.client(
        'MiN_Plate_to_Dataset.py',
        """This script converts Plates to Datasets.
All images are linked to a new Dataset and - if desired -
the link to the original Plate is cut.""",

        scripts.Int(
            "Plate_ID", grouping="1", optional=False,
            description="ID of the Plate"),

        scripts.String(
            "Dataset_name", grouping="2",
            description="Enter name of the new Dataset, or leave empty if the name of the original Plate should be used"),

        # planned functionality for a later time
        # scripts.Bool(
        #     "Remove_from_Plate", grouping="3", default=False,
        #     description="Remove Images from the original Plate as they are added to the new Dataset"),

        version="1",
        authors=["Jens Wendt", "Imaging Network WWU Muenster"],
        institutions=["WWU Muenster"],
        contact="wendtj@uni-muenster.de",
    )

    try:
        script_params = client.getInputs(unwrap=True)

        # wrap client to use the Blitz Gateway
        conn = BlitzGateway(client_obj=client)


        new_obj, message = plate_to_dataset(conn, script_params)

        client.setOutput("Message", rstring(message))
        if new_obj:
            client.setOutput("New_Object", robject(new_obj))

    finally:
        client.closeSession()
