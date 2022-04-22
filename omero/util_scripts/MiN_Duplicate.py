#!/usr/bin/env python
# -*- coding: utf-8 -*-

import omero.scripts as scripts
from omero.gateway import BlitzGateway
from omero.cmd import Duplicate
from omero.rtypes import rstring, rlist, rlong, robject
from collections import defaultdict


def duplicate(conn, script_params):
    dtype = script_params['Data_Type']
    ids = script_params['IDs']
    targets = defaultdict(list)
    for obj in conn.getObjects(dtype, ids):
        targets[dtype].append(obj.id)

    cmd = Duplicate()
    cmd.targetObjects = targets
    cb = conn.c.submit(cmd, loops=10, ms=3000)
    return cb


def run_script():
    """The main entry point of the script."""
    data_types = [rstring('Image'), rstring('Dataset')]

    client = scripts.client(
        'dupe.py',
        """
Duplicate objects using omero.cmd.Duplicate
    """,

        scripts.String(
            "Data_Type", optional=False, grouping="1",
            description="The data you want to work with.", values=data_types,
            default="Image"),

        scripts.List(
            "IDs", optional=False, grouping="2",
            description="List of Image or Dataset IDs").ofType(rlong(0)),

        version="0.1.0",
        authors=["OME Team"],
        institutions=["University of Dundee"],
        contact="ome-users@lists.openmicroscopy.org.uk",
    )

    try:
        conn = BlitzGateway(client_obj=client)
        script_params = client.getInputs(unwrap=True)
        cb = duplicate(conn, script_params)

        # objects = []
        # for dtype, ids in cb.getResponse().duplicates.items():
        #     if "." in dtype:
        #         dtype = dtype[dtype.rindex(".")+1:]
        #     objects.extend(conn.getObjects(dtype, ids))

        message = "Done"
        # if len(objects) == 0:
        #     message = ("Found no %ss with IDs: %s" % (script_params['Data_Type'], script_params['IDs']))
        # else:
        #     client.setOutput("Target", rlist([robject(obj._obj) for obj in objects]))
        client.setOutput("Message", rstring(message))

    finally:
        client.closeSession()


if __name__ == "__main__":
    run_script()