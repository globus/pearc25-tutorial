#!/usr/bin/env python

import argparse
import globus_sdk
import os
import random
import time
import uuid

# Globus Auth service account credentials; script will run as this Globus user
# TODO: Replace with your service account ID and secret
CLIENT_ID = "YOUR_SERVICE_ACCOUNT_ID"
CLIENT_SECRET = "YOUR_SERVICE_ACCOUNT_SECRET"

'''
The script will run flow actions as the authenticated user, 
i.e., using a Native App authentication flow. If CLIENT_SECRET is specified, 
the script will run flow actions as itself, i.e., as a service account
'''
if CLIENT_SECRET:
    GLOBUS_APP = globus_sdk.ClientApp(
        "instrument-flow-demo",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )
else:
    GLOBUS_APP = globus_sdk.UserApp(
        "instrument-flow-demo",
        client_id=CLIENT_ID,
    )
ac = globus_sdk.AuthClient(app=GLOBUS_APP)
info = ac.userinfo()
print(f"Trigger script is running as identity {info['name']} ({info['sub']})")


'''
Metadata generation.
In practice you might use another Compute action to
add experiment metadata and/or extract metadata from images
'''
def generate_search_metadata(
    filename=None, 
    sample_id=None, 
    run_time=None,
    hostname=None,
    share_path=None):

    instrument = random.choice(
        [
            {"model": "Leica SP2 LSCM", "format": "LIF"},
            {"model": "Olympus FV4000", "format": "TIFF"},
            {"model": "Zeiss Axio Observer 7", "format": "CZI"}
        ]
    )

    open_metadata = {
        "sample_id": sample_id,
        "sample_time": run_time,
        "instr_type": "Confocal",
        "instrument": instrument["model"],
        "image_format": instrument["format"],
        "spec_species": random.choice(["Rat", "Mouse", "Rabbit", "Possum", "Squirrel", "Lemming", "Mole", "Gerbil"]),
        "spec_organ": random.choice(["brain", "liver", "pancreas", "heart", "lung", "ovary", "kidney"]),
        "spec_sample": round(random.random()*1000000),
        "antifade_reagent": random.choice(["Phenylenediamine", "Tetramethylrhodamine", "TagRFP", "Fluorescein"]),
        "magnification": random.choice([100, 50, 1000]),
        "back_pinhole_nm": random.choice([490, 460, 430, 520, 550, 670]),
        "exc_wavelength_nm": random.choice([350, 495, 550, 575, 595, 652]),
        "em_wavelength_nm": random.choice([461, 519, 580, 603, 617, 688]),
        "x_axis": round(random.random()*1000.5, 3),
        "y_axis": round(random.random()*1000.5, 3),
        "z_axis": round(random.random()*1000.5, 3),
        "x_mpp": random.choice([1024, 4096, 8192]),
        "y_mpp": random.choice([1024, 4096, 8192]),
        "z_mbs": 1,
        "files": [
            {
                "filename": filename,
                "url": f"{hostname}{share_path}{filename}",
                "preview_url": f"{hostname}{share_path}results/thumb_{filename}",
            }
        ]
    }

    # Illustrates how metadata visibility can be restricted, apart from data
    # Restricted metadata are only visible only to Tutorial Users group members
    restricted_metadata = {
        "spec_age": random.choice(["60-90", "90-150", "150-210", "unknown"]),
        "compound": random.choice(["Haloperidol", "Chlorpromazine", "Paliperidone", "Aripiprazole", "Risperidone"]),
        "antidote": random.choice(["Acetylcysteine", "Atropine", "Digoxin", "Dimercaprol", "Flumazenil"]),
        "life_span": random.choice(["0-1", "1-2", "2-4", "4-6", "6+"])
    }

    return open_metadata, restricted_metadata


def run_flow(event_file):

    # TODO: Specify the flow to run when triggered
    flow_id = "REPLACE_WITH_FLOW_ID"
    
    # Instantiate a Globus Flows client for the specific flow ID above
    fc = globus_sdk.SpecificFlowClient(flow_id, app=GLOBUS_APP)

    # Rename the image file
    trigger_filename = f"{str(uuid.uuid4())[:13]}_{str(time.time())[:str(time.time()).find('.')]}.jpg"
    os.rename(event_file, trigger_filename)

    # Generate some identifying information/metadata
    sample_id = str(uuid.uuid4())  # unique identifier for differentiating demo runs
    run_time = str(time.time())[:str(time.time()).find('.')]

    # Set a label for the flow run
    flow_label = f"YOUR_TITLE_HERE: {sample_id}"

    # TODO: Modify source collection ID
    # Guest collection on the GCP endpoint where this trigger script is running
    source_id = "REPLACE_WITH_SOURCE_GUEST_COLLECTION_ID"

    # TODO: Modify the source path on the guest collection
    # The default is "/", assuming the guest collection base path is
    # the directory where images are captured
    source_path = "/"

    # TODO: Modify source collection ID
    # Guest collection on GCP endpoint on Compute node (rooted at ~/scratch)
    destination_id = "REPLACE_WITH_DESTINATION_GUEST_COLLECTION_ID"
    
    # TODO: Modify destination collection path
    destination_path = f"/{sample_id}/"

    # TODO: Modify Globus Compute endpoint ID
    compute_endpoint_id = "REPLACE_WITH_COMPUTE_ENDPOINT_ID"

    # TODO: Modify Globus Compute registered function ID
    compute_function_id = "REPLACE_WITH_REGISTERED_FUNCTION_ID"

    # Final data are published on Tutorial Guest Collection
    resultshare_id = "fe2feb64-4ac0-4a40-ba90-94b99d06dd2c"
    http_hostname = "https://g-13a6e8.f9e26.5898.data.globus.org"
    resultshare_path = f"/instrument-data/{sample_id}/"

    # Final data are accessible by members of the Tutorial Users group
    sharee_id = "50b6a29c-63ac-11e4-8062-22000ab68755"

    # Metadata are stored in this Globus Search index
    search_index = "510c9948-d915-4846-9929-47e31e84adc2"
    
    open_metadata, restricted_metadata = generate_search_metadata(
        filename=trigger_filename,
        sample_id=sample_id,
        run_time=run_time,
        hostname=http_hostname,
        share_path=resultshare_path
    )

    # Inputs to the flow
    flow_input = {
        "input": {
            "source": {
                "id": source_id,
                "path": source_path,
            },
            "destination": {
                "id": destination_id,
                "path": destination_path,
            },
            "trigger_filename": trigger_filename,
            "compute_endpoint_id": compute_endpoint_id,
            "compute_function_id": compute_function_id,
            "compute_function_kwargs": {
                "input_path": f"/home/dev2/scratch{destination_path}",
                "result_path": f"/home/dev2/scratch{destination_path}results",
            },
            "resultshare": {
                "id": resultshare_id,
                "path": resultshare_path,
            },
            "principal": {
                "type": "group",
                "id": sharee_id,
            },
             "search_ingest_document": {
                "search_index": search_index,
                "search_subject": f"{http_hostname}{resultshare_path}",
                "search_entry_id": f"{sample_id}-meta-open",
                "search_visible_to": ["public"],
                "search_content_metadata": open_metadata,
                "search_restricted_entry_id": f"{sample_id}-meta-restricted",
                "search_restricted_visible_to": [f"urn:globus:groups:id:{sharee_id}"],
                "search_content_restricted_metadata": restricted_metadata,
            },
        }
    }

    flow_run_request = fc.run_flow(
        body=flow_input,
        label=flow_label,
        tags=["Trigger_tutorial", "PEARC25"],  # TODO: Add your own tags here
    )
    print(
        f"\nTransferring and processing run ID {sample_id}: {open_metadata['spec_species']} {open_metadata['spec_organ']}"
    )
    print(
        f"Manage this run on Globus web app:\nhttps://app.globus.org/runs/{flow_run_request['run_id']}"
    )
    print(
        f"View published data:\nhttps://sc24-globus.github.io/experiment/search?q={sample_id}"
    )


# Parse input arguments
def parse_args():
    parser = argparse.ArgumentParser(
        description="""
        Run a compute job/task using Globus Compute and share results"""
    )
    parser.add_argument(
        "--watchdir",
        type=str,
        default=os.path.abspath("."),
        help=f"Directory path to watch. [default: current directory]",
    )
    parser.add_argument(
        "--patterns",
        type=str,
        default="",
        nargs="*",
        help='Filename suffix pattern(s) that will trigger the flow. [default: ""]',
    )
    parser.set_defaults(verbose=True)

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Creates and starts the watcher
    from watch import FileTrigger

    trigger = FileTrigger(
        watch_dir=os.path.expanduser(args.watchdir),
        patterns=args.patterns,
        FlowRunner=run_flow,
    )
    trigger.run()

