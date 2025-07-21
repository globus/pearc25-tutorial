A Globus flow and associated watchdog trigger script for processing an image. Used at the PEARC25 tutorial.

As illustrated in the picture below, the flow comprises the following actions:

1. Transfers the raw image from the instrument to a collection that's accessible from the Globus Compute endpoint.
1. Performs a simple analysis of the raw image by invoking a Globus Compute function.
1. Transfers the raw image and the results of the analysis to a guest collection.
1. Grants read-only access to the data to a Globus group.
1. Ingests metadata into a Globus Search index.
1. Ingests protected metadata (visible only to the sharee group members) into the Globus Search index.
1. Deletes the temporary (scratch) files from the collection used for computation.
1. Deletes the raw image from the collection on the instrument.

![Imsage showing the actions in a Globus flow that processes images coming from an instrument](/img/instrument_demostration_flow.png?raw=true "Instrument Data Processing Flow")

The flow is triggered by the creation of a file with the specified name/pattern in the directory that the [trigger script](/pearc25-tutorial/blob/main/trigger.py) is watching. For example, to run the flow when a JPG image file is created in the `/Microscope` directory, run:

`python .\trigger.py --watchdir /Microscope --patterns .jpg`

To run the flow you must edit the [trigger script](/pearc25-tutorial/blob/main/trigger.py) and provide/confirm the values for inputs flagged with a `# TODO` comment.